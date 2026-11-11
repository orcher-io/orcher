#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Multi-node cluster E2E harness (Gate B infrastructure — cluster-crate.md §8).
#
# The cluster analog of the dispatch stress harness: boot N orchestrator
# processes + M workers against ONE shared Postgres, drive K concurrent
# workflows through the real gRPC path, then assert **exactly-once execution**
# from the durable ledger (0 duplicate / 0 stranded) — the truth is the DB, not
# a process's own success report.
#
# It runs fully isolated (its own database + high ports) so it never collides
# with a dev orchestrator or another checkout sharing this machine.
#
#   Baseline (default): 2-node — prove exactly-once holds across two
#     orchestrators sharing Postgres (today's claim-based safety: workflow-tasks
#     are claimed FOR UPDATE SKIP LOCKED, so two orchestrators never double-run).
#   Failover (--fault kill): kill an orchestrator mid-flight and assert every
#     workflow STILL completes exactly once (the survivor + reclaimer pick up the
#     abandoned work). This is the split-brain / handover seam where correctness
#     bugs are easiest to introduce and hardest to notice.
#   Partitioned persistence (--shard-dbs N): boot the orchestrators with N
#     ADDITIONAL shard databases beyond the primary (total N+1 pools). Workflow
#     state is routed by shard across the pools (MultiPoolRouter), so the ledger
#     assertions aggregate across every database and additionally prove the
#     workflows actually DISTRIBUTE across the databases — the real out-scale proof
#     (each database is a separate instance, so aggregate write capacity grows).
#   Stress behind a per-request balancer (--stress): two clustered engines
#     behind scripts/grpc-rr-proxy, which sends EVERY gRPC call to the next
#     engine in turn, and the sdk-rust stress binary driving --workflows of them
#     through it. Consecutive calls from one worker land on different engines,
#     so about half of all workflow completions reach an engine that does not
#     own the workflow. With owner forwarding on (the default) that engine
#     relays them to the owner; --forwarding off shows what happens without it.
#     --fault kill kills engine 0 --kill-after seconds into the run. Reports the
#     stress result, fence refusals from the engine logs, and relay counts from
#     each engine's /metrics.
#
# Nothing here depends on the cluster crate yet — it exercises the ENGINE, so it
# is the substrate the fencing wiring will be validated against.
#
# Usage:
#   scripts/cluster-multinode-e2e.sh                 # 2 nodes, 4 workers, 20 workflows
#   scripts/cluster-multinode-e2e.sh --nodes 3 --workers 6 --workflows 40
#   scripts/cluster-multinode-e2e.sh --fault kill    # + kill a node mid-run
#   scripts/cluster-multinode-e2e.sh --shard-dbs 2   # partitioned persistence, 3 pools
#   scripts/cluster-multinode-e2e.sh --keep-db       # don't drop the test DB at end
#   scripts/cluster-multinode-e2e.sh --stress --workflows 1000
#   scripts/cluster-multinode-e2e.sh --stress --workflows 1000 --forwarding off
#   scripts/cluster-multinode-e2e.sh --stress --workflows 1000 --fault kill
#
# Prereqs (built once):
#   cargo build --release --bin orchestrator --bin orcher-migrate
#   an sdk-rust checkout beside this repository (or at SDK_RUST_DIR): the worker
#     is its durable_execution example, which the script builds
#   --stress: the sdk-rust stress binary (STRESS_BIN, default
#     $SDK_RUST_DIR/target/debug/stress: `cargo build -p orcher-contract --bin stress`
#     there); the proxy is built on first use.
# The engine binaries are looked up under $CARGO_TARGET_DIR when it is set.
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# ── Config (overridable via flags/env) ───────────────────────────────────────
NODES=2
WORKERS=4
WORKFLOWS=20
FAULT="none"                 # none | kill
CLUSTER=false                # --cluster: run orchestrators with ORCHER_CLUSTER_ENABLED
SHARD_DBS=0                  # --shard-dbs N: N additional shard databases (N+1 pools)
KEEP_DB=false
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-$(whoami)}"
TEST_DB="${ORCHER_CLUSTER_E2E_DB:-orcher_cluster_e2e}"
GRPC_BASE=50151             # node i → grpc GRPC_BASE+i, http HTTP_BASE+i
HTTP_BASE=50161
PG_MAX_CONN=25             # per orchestrator; N*PG_MAX_CONN must stay < PG max_connections
NAMESPACE="examples"
TASK_QUEUE="durable-execution-queue"
WORKFLOW_TIMEOUT=90        # seconds to wait for all workflows to reach terminal
STRESS=false               # --stress: engines behind the per-request proxy, stress binary
FORWARDING=on              # --forwarding on|off: ORCHER_CLUSTER_FORWARDING on clustered engines
KILL_AFTER=15              # --kill-after N: with --stress --fault kill, seconds into the run
STRESS_DEADLINE=180        # --deadline N: the stress binary's --deadline-secs
PROXY_PORT=50170

TARGET_DIR="${CARGO_TARGET_DIR:-target}"
ORCH_BIN="$TARGET_DIR/release/orchestrator"
MIGRATE_BIN="$TARGET_DIR/release/orcher-migrate"
SDK_RUST_DIR="${SDK_RUST_DIR:-$PROJECT_ROOT/../sdk-rust}"
EXAMPLE_BIN="$SDK_RUST_DIR/target/release/durable-execution-service"
STRESS_BIN="${STRESS_BIN:-$SDK_RUST_DIR/target/debug/stress}"
PROXY_BIN="${PROXY_BIN:-$TARGET_DIR/grpc-rr-proxy/release/grpc-rr-proxy}"

while [ $# -gt 0 ]; do
    case "$1" in
        --nodes)      NODES="$2"; shift 2 ;;
        --workers)    WORKERS="$2"; shift 2 ;;
        --workflows)  WORKFLOWS="$2"; shift 2 ;;
        --fault)      FAULT="$2"; shift 2 ;;
        --cluster)    CLUSTER=true; shift ;;
        --shard-dbs)  SHARD_DBS="$2"; shift 2 ;;
        --keep-db)    KEEP_DB=true; shift ;;
        --stress)     STRESS=true; CLUSTER=true; shift ;;
        --forwarding) FORWARDING="$2"; shift 2 ;;
        --kill-after) KILL_AFTER="$2"; shift 2 ;;
        --deadline)   STRESS_DEADLINE="$2"; shift 2 ;;
        -h|--help)    grep '^#' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

LOG_DIR="$(mktemp -d -t orcher-cluster-e2e-XXXXXX)"
ORCH_PIDS=()
WORKER_PIDS=()
PROXY_PID=""

log()   { printf '\033[0;36m[harness]\033[0m %s\n' "$*"; }
ok()    { printf '\033[0;32m[  ok  ]\033[0m %s\n' "$*"; }
fail()  { printf '\033[0;31m[ FAIL ]\033[0m %s\n' "$*"; }

DBURL_BASE="postgresql://${PGUSER}@${PGHOST}:${PGPORT}"
TEST_DBURL="${DBURL_BASE}/${TEST_DB}"

# ── Shard databases (partitioned persistence) ────────────────────────────────
# The primary (TEST_DB) is shard-pool 0 (also the coordination DB). Each extra
# shard DB is an additional pool; the orchestrators receive them via
# ORCHER_SHARD_DATABASE_URLS and a MultiPoolRouter splits the shard space across
# [primary, shard_1, …]. SHARD_DBS=0 ⇒ ALL_DBURLS is just the primary, so every
# aggregate below is byte-identical to the single-database path.
SHARD_DB_NAMES=()
SHARD_DBURLS=()
# NB: guard on >0 — macOS BSD `seq 1 0` prints "1\n0" (descending), not empty, so an
# unguarded loop would fabricate shard DBs for the single-DB (SHARD_DBS=0) case.
if [ "$SHARD_DBS" -gt 0 ]; then
    for s in $(seq 1 "$SHARD_DBS"); do
        name="${TEST_DB}_s${s}"
        SHARD_DB_NAMES+=("$name")
        SHARD_DBURLS+=("${DBURL_BASE}/${name}")
    done
fi
# Comma-joined for ORCHER_SHARD_DATABASE_URLS (empty when SHARD_DBS=0).
SHARD_URLS_CSV=""
if [ "${#SHARD_DBURLS[@]}" -gt 0 ]; then
    SHARD_URLS_CSV=$(IFS=,; echo "${SHARD_DBURLS[*]}")
fi
# Every pool, primary first, for aggregate ledger queries.
ALL_DBURLS=("$TEST_DBURL")
for u in ${SHARD_DBURLS[@]+"${SHARD_DBURLS[@]}"}; do ALL_DBURLS+=("$u"); done

psql_test() { psql "$TEST_DBURL" -tA -c "$1"; }

# Sum a scalar (COUNT) query across every pool database. A workflow's rows live
# entirely on its shard DB (routing is deterministic by workflow_id), so summing
# per-DB counts — including COUNT(DISTINCT execution_id) — yields the global total.
psql_sum() {
    local q="$1" sum=0 v
    for u in "${ALL_DBURLS[@]}"; do
        v=$(psql "$u" -tA -c "$q" 2>/dev/null)
        sum=$((sum + ${v:-0}))
    done
    echo "$sum"
}

cleanup() {
    log "cleaning up…"
    # bash 3.2 (macOS): guard empty-array expansion under `set -u`.
    for pid in ${WORKER_PIDS[@]+"${WORKER_PIDS[@]}"} ${ORCH_PIDS[@]+"${ORCH_PIDS[@]}"} $PROXY_PID; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    if [ "$KEEP_DB" = false ]; then
        dropdb --if-exists -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$TEST_DB" 2>/dev/null || true
        for name in ${SHARD_DB_NAMES[@]+"${SHARD_DB_NAMES[@]}"}; do
            dropdb --if-exists -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$name" 2>/dev/null || true
        done
    else
        log "kept test DB: $TEST_DB ${SHARD_DB_NAMES[@]+${SHARD_DB_NAMES[@]}}"
    fi
    log "logs: $LOG_DIR"
}
trap cleanup EXIT INT TERM

# Wait until a TCP port accepts a connection (orchestrator readiness).
wait_for_port() {
    local port="$1" name="$2" tries=60
    while [ $tries -gt 0 ]; do
        if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then exec 3>&- 3<&-; return 0; fi
        sleep 0.5; tries=$((tries-1))
    done
    fail "$name did not open port $port"; return 1
}

# ── Preflight ────────────────────────────────────────────────────────────────
if [ "$STRESS" = true ]; then
    if [ ! -x "$PROXY_BIN" ]; then
        log "building the per-request proxy…"
        cargo build --release --manifest-path scripts/grpc-rr-proxy/Cargo.toml \
            --target-dir "$TARGET_DIR/grpc-rr-proxy" > "$LOG_DIR/proxy-build.log" 2>&1 \
            || { fail "proxy build failed — see $LOG_DIR/proxy-build.log"; exit 1; }
    fi
    REQUIRED_BINS=("$ORCH_BIN" "$MIGRATE_BIN" "$STRESS_BIN" "$PROXY_BIN")
else
    if [ ! -f "$SDK_RUST_DIR/examples/durable_execution/Cargo.toml" ]; then
        fail "sdk-rust checkout not found at $SDK_RUST_DIR — clone https://github.com/orcher-io/sdk-rust beside this repository, or set SDK_RUST_DIR"
        exit 1
    fi
    log "building the worker (sdk-rust durable_execution example)…"
    cargo build --release --manifest-path "$SDK_RUST_DIR/Cargo.toml" \
        --target-dir "$SDK_RUST_DIR/target" -p orcher-example-durable-execution \
        > "$LOG_DIR/worker-build.log" 2>&1 \
        || { fail "worker build failed — see $LOG_DIR/worker-build.log"; exit 1; }
    REQUIRED_BINS=("$ORCH_BIN" "$MIGRATE_BIN" "$EXAMPLE_BIN")
fi
for bin in "${REQUIRED_BINS[@]}"; do
    [ -x "$bin" ] || { fail "missing binary: $bin — see prereqs (--help)"; exit 1; }
done

log "config: nodes=$NODES workers=$WORKERS workflows=$WORKFLOWS fault=$FAULT db=$TEST_DB"

# ── Fresh isolated database(s) ───────────────────────────────────────────────
dropdb --if-exists -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$TEST_DB" 2>/dev/null || true
createdb -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$TEST_DB" || { fail "createdb"; exit 1; }
for name in ${SHARD_DB_NAMES[@]+"${SHARD_DB_NAMES[@]}"}; do
    dropdb --if-exists -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$name" 2>/dev/null || true
    createdb -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$name" || { fail "createdb $name"; exit 1; }
done
log "migrating $((SHARD_DBS+1)) pool(s)…"
# Each pool gets the full versioned schema. `orcher-migrate run` reads DATABASE_URL,
# so we invoke it once per pool (primary + each shard DB) — self-contained and
# independent of any multi-URL support in the binary.
midx=0
for u in "${ALL_DBURLS[@]}"; do
    DATABASE_URL="$u" "$MIGRATE_BIN" run > "$LOG_DIR/migrate-${midx}.log" 2>&1 \
        || { fail "migration failed on pool $midx — see $LOG_DIR/migrate-${midx}.log"; exit 1; }
    midx=$((midx+1))
done
# Seed the workflow namespace on every pool (a fresh DB has none; the orchestrator
# rejects start-workflow for an unknown/inactive namespace, and a workflow's row is
# written to its shard DB — so each shard needs the namespace too).
for u in "${ALL_DBURLS[@]}"; do
    psql "$u" -tA -c "INSERT INTO namespaces (name, description, retention_period_days, status)
               SELECT '${NAMESPACE}', 'cluster e2e harness', 7, 'active'
               WHERE NOT EXISTS (SELECT 1 FROM namespaces WHERE name='${NAMESPACE}');" > /dev/null \
        || { fail "namespace seed failed"; exit 1; }
done
ok "database(s) migrated + namespace '${NAMESPACE}' seeded across $((SHARD_DBS+1)) pool(s)"

# ── Boot orchestrators ───────────────────────────────────────────────────────
# --cluster turns on the membership loop (each node registers in cluster_members
# and heartbeats). It gates only membership, not the write path, so the
# exactly-once ledger assertions below must hold identically either way.
for i in $(seq 0 $((NODES-1))); do
    grpc=$((GRPC_BASE+i)); http=$((HTTP_BASE+i))
    cluster_env=()
    cluster_note=""
    if [ "$CLUSTER" = true ]; then
        # Each engine advertises the address its peers relay to it on; the bind
        # address (0.0.0.0) is not one they can dial.
        cluster_env=(ORCHER_CLUSTER_ENABLED=true ORCHER_NODE_ID="node-${i}" ORCHER_CLUSTER_HEARTBEAT_SECS=2
                     ORCHER_ADVERTISE_ADDR="127.0.0.1:${grpc}" ORCHER_CLUSTER_FORWARDING="$FORWARDING")
        cluster_note=" cluster=on forwarding=$FORWARDING"
    fi
    # Cluster vars go through `env` (expanded array words aren't recognized as
    # bare VAR=val command prefixes; `env` accepts them as arguments).
    # ORCHER_SHARD_DATABASE_URLS (empty ⇒ single-pool) turns on partitioned
    # persistence: writes/reads/recovery route by shard across [primary, shard_1, …].
    DATABASE_URL="$TEST_DBURL" \
    ORCHER_SHARD_DATABASE_URLS="$SHARD_URLS_CSV" \
    GRPC_BIND_ADDR="0.0.0.0:${grpc}" \
    HTTP_BIND_ADDR="0.0.0.0:${http}" \
    ORCHER_INSECURE=true \
    ORCHER_PG_MAX_CONNECTIONS="$PG_MAX_CONN" \
    ORCHER_PG_MIN_CONNECTIONS=5 \
        env ${cluster_env[@]+"${cluster_env[@]}"} "$ORCH_BIN" > "$LOG_DIR/orch-${i}.log" 2>&1 &
    opid=$!
    ORCH_PIDS+=($opid)
    log "orchestrator $i → grpc :$grpc http :$http (pid $opid)${cluster_note}"
done
for i in $(seq 0 $((NODES-1))); do
    wait_for_port $((GRPC_BASE+i)) "orchestrator $i" || exit 1
done
ok "$NODES orchestrators listening"

# ── Stress behind the per-request proxy ──────────────────────────────────────
# Replaces the worker/client phase below: the stress binary is both the worker
# and the client, and every one of its calls goes through the proxy.
if [ "$STRESS" = true ]; then
    backends=()
    for i in $(seq 0 $((NODES-1))); do backends+=("127.0.0.1:$((GRPC_BASE+i))"); done
    "$PROXY_BIN" "127.0.0.1:${PROXY_PORT}" "${backends[@]}" > "$LOG_DIR/proxy.log" 2>&1 &
    PROXY_PID=$!
    wait_for_port "$PROXY_PORT" "proxy" || exit 1
    ok "proxy :$PROXY_PORT → ${backends[*]} (per request, round robin)"

    # Each engine's relay counts, read before a killed engine takes them along.
    scrape_forwarded() {
        for i in $(seq 0 $((NODES-1))); do
            curl -s "http://127.0.0.1:$((HTTP_BASE+i))/metrics" 2>/dev/null \
                | grep '^orcher_forwarded_requests_total' | sed "s/^/  node-$i /" \
                > "$LOG_DIR/forwarded-$i.txt" || true
        done
    }

    log "stress: $WORKFLOWS workflows, deadline ${STRESS_DEADLINE}s, forwarding=$FORWARDING fault=$FAULT"
    RUST_LOG="${STRESS_RUST_LOG:-warn}" "$STRESS_BIN" \
        --server-url "http://127.0.0.1:${PROXY_PORT}" \
        --namespace "$NAMESPACE" --task-queue dispatch-stress \
        --count "$WORKFLOWS" --deadline-secs "$STRESS_DEADLINE" > "$LOG_DIR/stress.log" 2>&1 &
    stress_pid=$!
    if [ "$FAULT" = "kill" ] && [ "$NODES" -ge 2 ]; then
        sleep "$KILL_AFTER"
        scrape_forwarded
        victim="${ORCH_PIDS[0]}"
        log "FAULT: killing orchestrator 0 (pid $victim) ${KILL_AFTER}s into the run"
        kill -9 "$victim" 2>/dev/null || true
        ORCH_PIDS=("${ORCH_PIDS[@]:1}")
    fi
    wait "$stress_pid"; stress_rc=$?
    [ "$FAULT" = "kill" ] || scrape_forwarded
    # Keep the survivor's count current in the kill run too.
    if [ "$FAULT" = "kill" ]; then
        for i in $(seq 1 $((NODES-1))); do
            curl -s "http://127.0.0.1:$((HTTP_BASE+i))/metrics" 2>/dev/null \
                | grep '^orcher_forwarded_requests_total' | sed "s/^/  node-$i /" \
                > "$LOG_DIR/forwarded-$i.txt" || true
        done
    fi

    echo
    log "── stress (exit $stress_rc) ──"
    grep -E 'workflows completed|distinct tasks|total task executions|duplicate executions|stranded|wall clock|DISPATCH STRESS' \
        "$LOG_DIR/stress.log" | sed 's/^/  /'
    log "── fence refusals (engine logs) ──"
    for i in $(seq 0 $((NODES-1))); do
        printf '  node-%s: %s\n' "$i" "$(grep -c 'shard ownership lost' "$LOG_DIR/orch-${i}.log" 2>/dev/null || true)"
    done
    log "── relayed to the owner (/metrics) ──"
    cat "$LOG_DIR"/forwarded-*.txt 2>/dev/null || true
    # The stress binary already waited out its deadline; what is not terminal now
    # is unfinished, so the ledger below reads it as it stands.
    WORKFLOW_TIMEOUT=5
else

# ── Boot workers (split across nodes) ────────────────────────────────────────
for w in $(seq 0 $((WORKERS-1))); do
    node=$((w % NODES)); grpc=$((GRPC_BASE+node))
    ORCHER_SERVER_URL="http://localhost:${grpc}" \
    ORCHER_NAMESPACE="$NAMESPACE" \
    ORCHER_TASK_QUEUE="$TASK_QUEUE" \
    ORCHER_WORKER_ID="worker-${w}" \
    ORCHER_WORKFLOW_POLLER_COUNT=6 \
    ORCHER_TASK_POLLER_COUNT=6 \
        "$EXAMPLE_BIN" > "$LOG_DIR/worker-${w}.log" 2>&1 &
    WORKER_PIDS+=($!)
done
log "started $WORKERS workers across $NODES nodes"
sleep 3   # let workers register + start polling
ok "workers polling"

# ── Drive K workflows (client submissions) ───────────────────────────────────
# Baseline: split submissions across all nodes. Fault mode: route EVERY
# submission through the survivor (the highest-index node), so we test execution
# *failover* (the killed node's claimed work must be reclaimed) rather than
# submission failure against a node that's about to die.
log "submitting $WORKFLOWS workflows…"
CLIENT_PIDS=()
for k in $(seq 0 $((WORKFLOWS-1))); do
    if [ "$FAULT" = "kill" ]; then node=$((NODES-1)); else node=$((k % NODES)); fi
    grpc=$((GRPC_BASE+node))
    ORCHER_SERVER_URL="http://localhost:${grpc}" \
    ORCHER_NAMESPACE="$NAMESPACE" \
    ORCHER_TASK_QUEUE="$TASK_QUEUE" \
        "$EXAMPLE_BIN" --client --skip-idempotency-check > "$LOG_DIR/client-${k}.log" 2>&1 &
    CLIENT_PIDS+=($!)
done

# ── Fault injection (optional): kill node 0 mid-flight ───────────────────────
if [ "$FAULT" = "kill" ] && [ "$NODES" -ge 2 ]; then
    sleep 2
    victim="${ORCH_PIDS[0]}"
    log "FAULT: killing orchestrator 0 (pid $victim) mid-flight"
    kill -9 "$victim" 2>/dev/null || true
    ORCH_PIDS=("${ORCH_PIDS[@]:1}")   # drop from cleanup list
    ok "orchestrator 0 killed — survivors must finish the work"
fi

# Clients may fail their own wait if their node was killed; the DB is the truth.
for pid in "${CLIENT_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
log "all client submissions returned"
fi

# ── Poll the ledger until every workflow reaches a terminal state ────────────
# Failover needs longer: the killed node's claimed work only moves after its
# claims expire and the survivor's reclaimer repositions it.
[ "$FAULT" = "kill" ] && [ "$STRESS" = false ] && WORKFLOW_TIMEOUT=180
log "waiting for workflows to reach terminal state (≤${WORKFLOW_TIMEOUT}s)…"
deadline=$((SECONDS+WORKFLOW_TIMEOUT))
while [ $SECONDS -lt $deadline ]; do
    pending=$(psql_sum "SELECT COUNT(*) FROM workflow_executions WHERE namespace='${NAMESPACE}' AND status NOT IN ('completed','failed','cancelled','terminated','timed_out');")
    total=$(psql_sum "SELECT COUNT(*) FROM workflow_executions WHERE namespace='${NAMESPACE}';")
    [ "${pending:-1}" = "0" ] && [ "${total:-0}" -ge "$WORKFLOWS" ] && break
    sleep 1
done

# ── Assertions (the exactly-once ledger) ─────────────────────────────────────
echo
log "── ledger ──"
total=$(psql_sum "SELECT COUNT(*) FROM workflow_executions WHERE namespace='${NAMESPACE}';")
completed=$(psql_sum "SELECT COUNT(*) FROM workflow_executions WHERE namespace='${NAMESPACE}' AND status='completed';")
nonterminal=$(psql_sum "SELECT COUNT(*) FROM workflow_executions WHERE namespace='${NAMESPACE}' AND status NOT IN ('completed','failed','cancelled','terminated','timed_out');")
distinct=$(psql_sum "SELECT COUNT(DISTINCT execution_id) FROM workflow_executions WHERE namespace='${NAMESPACE}';")
printf '  submitted=%s  rows=%s  completed=%s  distinct_ids=%s  non_terminal=%s\n' \
    "$WORKFLOWS" "$total" "$completed" "$distinct" "$nonterminal"

rc=0
[ "$completed" = "$WORKFLOWS" ]   || { fail "expected $WORKFLOWS completed, got $completed"; rc=1; }
[ "$distinct"  = "$total" ]       || { fail "duplicate execution rows: $total rows vs $distinct distinct ids"; rc=1; }
[ "$nonterminal" = "0" ]          || { fail "$nonterminal workflows stranded (never reached terminal)"; rc=1; }
[ "$total" = "$WORKFLOWS" ]       || { fail "row count $total != submitted $WORKFLOWS (dup or lost)"; rc=1; }

# With --cluster, also assert the membership loop registered every node (each
# registers at startup before serving; a later-killed node keeps its row).
if [ "$CLUSTER" = true ]; then
    members=$(psql_test "SELECT COUNT(*) FROM cluster_members;")
    printf '  cluster_members=%s (expected ≥ %s)\n' "$members" "$NODES"
    [ "${members:-0}" -ge "$NODES" ] || { fail "cluster membership: $members members registered, expected ≥ $NODES"; rc=1; }
fi

# With partitioned persistence, additionally prove the workflows actually SPREAD
# across the databases (not all funneled to one) — the out-scale property. Each
# pool holds a contiguous shard range; with equal ranges and random workflow ids
# every pool should carry work.
if [ "$SHARD_DBS" -gt 0 ]; then
    echo
    log "── shard distribution ($((SHARD_DBS+1)) pools) ──"
    idx=0
    pools_with_work=0
    for u in "${ALL_DBURLS[@]}"; do
        c=$(psql "$u" -tA -c "SELECT COUNT(*) FROM workflow_executions WHERE namespace='${NAMESPACE}';" 2>/dev/null)
        printf '  pool[%s] rows=%s\n' "$idx" "${c:-0}"
        [ "${c:-0}" -ge 1 ] && pools_with_work=$((pools_with_work+1))
        idx=$((idx+1))
    done
    [ "$pools_with_work" = "$((SHARD_DBS+1))" ] \
        || { fail "workflows not distributed: only $pools_with_work/$((SHARD_DBS+1)) pools hold work"; rc=1; }
fi

echo
if [ $rc -eq 0 ]; then
    if [ "$SHARD_DBS" -gt 0 ]; then
        ok "EXACTLY-ONCE + DISTRIBUTED across $((SHARD_DBS+1)) shard DBs / $NODES node(s) (fault=$FAULT): $completed/$WORKFLOWS, 0 dup, 0 stranded"
    else
        ok "EXACTLY-ONCE HELD across $NODES-node cluster (fault=$FAULT): $completed/$WORKFLOWS, 0 dup, 0 stranded"
    fi
else
    fail "exactly-once VIOLATED — see logs in $LOG_DIR"
fi
exit $rc
