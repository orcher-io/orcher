#!/usr/bin/env bash
# orcher on Kubernetes — cluster disruption verification.
#
# Stands up a 3-node k3d cluster with Postgres + N clustered orchestrator
# pods, drives workflows through them from an in-cluster load harness, disrupts
# the cluster mid-flight, and asserts every workflow still completes EXACTLY-ONCE
# (0 duplicate, 0 permanently-stranded). This is a correctness gate, not a
# throughput benchmark.
#
# SCENARIOS (SCENARIO=...):
#
#   kill      (default) — delete an orchestrator pod mid-flight. The dead node's
#               shards are reclaimed by the survivors once its heartbeat goes
#               stale. Exercises: reclaim-from-dead-owner.
#
#   scale-up            — add an orchestrator pod mid-flight. The contiguous
#               range assignment shifts boundaries, moving shards to the newcomer
#               while the previous owners are still ALIVE, so this exercises a
#               different and stricter path:
#               ownership must hand off between two live nodes. acquire_shard
#               only succeeds when the current owner is NULL or dead, so the
#               newcomer cannot seize a shard — it waits for the prior owner to
#               notice the membership change and release on its next maintenance
#               tick (ORCHER_CLUSTER_HEARTBEAT_SECS, default 5).
#               Exercises: handoff-between-live-owners.
#
# Correctness vs liveness: a duplicate execution is a HARD failure (exactly-once
# violated). The completion rate is reported and checked against a floor
# (MIN_COMPLETION_PCT, default 90) rather than a strict 100%, because the load
# harness caps each workflow's wait at 60s — see the note above the run loop.
#
# Prerequisites: colima running (`colima start`), docker + k3d + kubectl on PATH,
# and the sibling sdk-rust checkout. See README.md.
#
# Usage:
#   ./failover-test.sh                 # build images if missing, run once (graceful kill)
#   SCENARIO=scale-up ./failover-test.sh    # add a pod mid-flight instead of killing one
#   SCENARIO=seize ./failover-test.sh       # supersede a LIVE node's shards (fencing)
#   KILL_MODE=force ./failover-test.sh # SIGKILL crash instead of graceful drain
#   RUNS=5 ./failover-test.sh          # repeat the scenario 5 times
#   COUNT=500 MIN_COMPLETION_PCT=100 ./failover-test.sh   # on a bigger runner
#   ./failover-test.sh --rebuild       # force-rebuild images first
#   ./failover-test.sh --cleanup       # delete the cluster and exit
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"                 # the orcher repo root
SDK_RUST="$(cd "$HERE/../../../../sdk-rust" 2>/dev/null && pwd || true)"  # sibling sdk-rust repo

CLUSTER=${CLUSTER:-orcher-ha}
NS=orcher
ORCH_IMAGE=orcher/orchestrator:local
STRESS_IMAGE=orcher/stress:local
# COUNT must fit the load harness's PER-WORKFLOW result wait, which is hardcoded
# to 60s in the harness (sdk-rust contract stress.rs) and is NOT extended by
# --deadline-secs. A workflow queued behind the rest past its own 60s is counted
# as an error even though the engine would have finished it. On the reference
# runner (4-CPU VM, in-cluster Postgres on emptyDir) throughput is ~2-20
# workflows/sec, so 100 leaves margin at the low end. Raise it on a bigger runner.
# The seize scenario needs the workload to outlast the disruption: a node only
# takes a shard on its first guarded write, so with a short run the batch can
# drain before any node owns anything to seize. Default it higher.
if [ "${SCENARIO:-kill}" = "seize" ]; then COUNT=${COUNT:-400}; else COUNT=${COUNT:-100}; fi
REPLICAS=${REPLICAS:-3}
SCENARIO=${SCENARIO:-kill}           # kill | scale-up
KILL_MODE=${KILL_MODE:-graceful}     # graceful | force  (kill scenario only)
# How far into the run the disruption happens. KILL_DELAY is kept as an alias so
# existing invocations keep working.
DISRUPT_DELAY=${DISRUPT_DELAY:-${KILL_DELAY:-8}}
KILL_DELAY=$DISRUPT_DELAY
# Client connect timeout for the harness. Does NOT change the 60s per-workflow
# wait described above.
DEADLINE=${DEADLINE:-240}
RUNS=${RUNS:-1}

log(){ printf '\n\033[0;36m=== %s ===\033[0m\n' "$*"; }
die(){ printf '\033[0;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }
# Every kubectl call is pinned to the cluster this harness creates. Without the
# --context the commands below would follow whatever context the operator
# happens to have selected — which, for anyone with a production cluster in
# their kubeconfig, is a genuinely bad way to find out.
KUBECTX="k3d-${CLUSTER}"
kc(){ kubectl --context "$KUBECTX" -n "$NS" "$@"; }

if [ "${1:-}" = "--cleanup" ]; then log "deleting cluster $CLUSTER"; k3d cluster delete "$CLUSTER"; exit 0; fi
REBUILD=false; [ "${1:-}" = "--rebuild" ] && REBUILD=true

# ---- prerequisites -----------------------------------------------------------
for bin in docker k3d kubectl; do command -v "$bin" >/dev/null || die "missing '$bin' on PATH"; done
docker info >/dev/null 2>&1 || die "docker daemon not reachable — start the runtime (e.g. 'colima start')"
[ -n "$SDK_RUST" ] || die "sibling sdk-rust repo not found at $HERE/../../../../sdk-rust (needed to build the load harness image)"

# ---- images ------------------------------------------------------------------
# Build only when missing (or --rebuild). Neither image needs a token: every
# dependency comes from crates.io.
build_images(){
  # keep the build contexts small
  grep -q '^target/' "$REPO_ROOT/.dockerignore" 2>/dev/null || printf 'target/\n**/target/\n.git/\n' > "$REPO_ROOT/.dockerignore"
  grep -q '^target/' "$SDK_RUST/.dockerignore" 2>/dev/null || printf 'target/\n**/target/\n.git/\n' > "$SDK_RUST/.dockerignore"
  log "building $ORCH_IMAGE (orchestrator + orcher-migrate)"
  docker build -f "$HERE/Dockerfile" -t "$ORCH_IMAGE" "$REPO_ROOT" || die "orchestrator image build failed"
  log "building $STRESS_IMAGE (in-cluster load harness)"
  docker build -f "$HERE/Dockerfile.stress" -t "$STRESS_IMAGE" "$SDK_RUST" || die "stress image build failed"
}
# Reuse is only safe while the images are newer than the code. They were not:
# a run on 2026-08-16 passed against an orchestrator image built on 2026-08-14,
# so the gate silently tested code from before the write fence was wired to the
# repository — and the "shards acquired: 0" line looked like a dormant fence
# rather than a stale binary. A gate that quietly tests old code is worse than a
# slow one, so staleness now forces a rebuild.
# Is any engine source newer than the image? Keyed on the Rust sources rather
# than the git HEAD, so editing this harness does not trigger a rebuild while a
# one-line change to the engine still does.
image_older_than_sources(){
  local img=$1 built
  built=$(docker image inspect "$img" --format '{{.Created}}' 2>/dev/null) || return 0
  [ -n "$(find "$REPO_ROOT/crates" "$REPO_ROOT/services" "$REPO_ROOT/Cargo.toml" \
            -name '*.rs' -o -name 'Cargo.toml' 2>/dev/null \
          | xargs -I{} find {} -newermt "${built%.*}" -print -quit 2>/dev/null | head -1)" ]
}
if $REBUILD || ! docker image inspect "$ORCH_IMAGE" >/dev/null 2>&1 || ! docker image inspect "$STRESS_IMAGE" >/dev/null 2>&1; then
  build_images
elif [ "${REUSE_IMAGES:-false}" != "true" ] && image_older_than_sources "$ORCH_IMAGE"; then
  log "orchestrator image predates the engine sources — rebuilding (REUSE_IMAGES=true to skip)"
  build_images
else
  log "reusing existing images ($ORCH_IMAGE built $(docker image inspect "$ORCH_IMAGE" --format '{{.Created}}' 2>/dev/null))"
fi

# ---- cluster -----------------------------------------------------------------
log "create k3d cluster '$CLUSTER' (1 server + 2 agents)"
k3d cluster delete "$CLUSTER" 2>/dev/null || true
k3d cluster create "$CLUSTER" --servers 1 --agents 2 --wait || die "cluster create failed"
log "import images into the cluster"
k3d image import "$ORCH_IMAGE" "$STRESS_IMAGE" -c "$CLUSTER" || die "image import failed"

# ---- deploy ------------------------------------------------------------------
# One apply of the overlay, which layers a test-only Postgres on top of the
# REAL production bundle (../deploy) — orchestrator Deployment, migration Job,
# Service, RBAC, HPA and PDB all come from there. Nothing about the engine's
# deployment is test-specific, so this gate validates what we actually ship.
log "apply overlay (production bundle + test infra)"
kubectl --context "$KUBECTX" apply -k "$HERE" || die "kubectl apply -k failed"
kc rollout status deploy/postgres --timeout=180s || die "postgres not ready"

# The production migration Job has no DB-wait (a production database already
# exists); here it may retry a few times while Postgres finishes starting, which
# also proves the bundle is self-healing on ordering.
log "wait for migration Job"
for _ in $(seq 1 60); do
  [ "$(kc get job orcher-migrate -o jsonpath='{.status.succeeded}' 2>/dev/null)" = "1" ] && break
  sleep 5
done
[ "$(kc get job orcher-migrate -o jsonpath='{.status.succeeded}' 2>/dev/null)" = "1" ] \
  || die "migration job did not complete: kubectl -n $NS logs job/orcher-migrate"

log "wait for $REPLICAS clustered orchestrators"
kc scale deploy/orchestrator --replicas="$REPLICAS" >/dev/null 2>&1
kc rollout status deploy/orchestrator --timeout=240s || die "orchestrators not ready"
PGPOD=$(kc get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}')
log "cluster_members (expect $REPLICAS)"; kc exec "$PGPOD" -- psql -U orcher -d orcher -tAc "SELECT count(*) FROM cluster_members;" 2>/dev/null || true

# ---- disruptions -------------------------------------------------------------
# Each disrupt_* runs mid-flight, in the background, while the load harness is
# driving workflows. Adding a scenario is one function plus one case in
# disrupt(); nothing else in the harness needs to know about it.

# Delete a pod. Its shards are reclaimed once its heartbeat goes stale, because
# acquire_shard admits a new owner when the current one is NULL or dead.
disrupt_kill(){
  local victim; victim=$(kc get pod -l app=orchestrator -o jsonpath='{.items[0].metadata.name}')
  log "  disrupt: ${KILL_MODE} kill of $victim"
  if [ "$KILL_MODE" = "force" ]; then kc delete pod "$victim" --grace-period=0 --force >/dev/null 2>&1
  else kc delete pod "$victim" >/dev/null 2>&1; fi
}

# Add a pod. The range assignment shifts boundaries onto it while their
# current owners are still ALIVE — so, unlike a kill, no shard can be seized.
# The newcomer must wait for each prior owner to observe the membership change
# and release on its next maintenance tick. That is the path under test.
disrupt_scale_up(){
  local to=$((REPLICAS + 1))
  log "  disrupt: scale $REPLICAS -> $to (handoff between live owners)"
  kc scale deploy/orchestrator --replicas="$to" >/dev/null 2>&1
  kc rollout status deploy/orchestrator --timeout=180s >/dev/null 2>&1 \
    || log "  warn: scaled pod not ready before the deadline"
  kc exec "$PGPOD" -- psql -U orcher -d orcher -tAc \
    "SELECT count(*) FROM cluster_members WHERE status='active';" 2>/dev/null \
    | tr -d '[:space:]' | sed 's/^/  active cluster_members after scale-up: /'
}

# Churn shard ownership under a live cluster: repeatedly reassign the shards a
# node owns to another member and bump their generation, without telling it.
#
# WHAT THIS PROVES: exactly-once holds while ownership moves continuously. A
# measured run reassigned ownership 4,460 times across 20 rounds during 400
# workflows and produced 0 duplicates at 100% completion.
#
# WHAT IT DOES NOT PROVE: that the write fence refuses a superseded write. It
# was written to show that, and it does not, which is worth stating plainly
# rather than leaving a reader to assume a green run covers it:
#
#   * Claim scoping sits UPSTREAM of the fence. Once a node's shards are taken
#     it stops being handed work at all, so it never reaches a write for the
#     fence to refuse. Measured: 0 refusals even under continuous churn.
#   * The fence therefore guards a narrower case — a write already in flight at
#     the instant ownership moves. That window is a guard/commit pair, well
#     under a millisecond, and seize rounds land seconds apart. Hitting it
#     reliably from a shell harness is not realistic.
#
# So the fence's refusal behaviour is evidenced by the six live-database tests
# in crates/core/tests/write_fence_refuses_stale_owner.rs, which construct the
# superseded state directly. This scenario covers what those cannot: that the
# ownership machinery itself survives churn without losing exactly-once.
disrupt_seize(){
  local victim other seized
  # Ownership is acquired lazily — a node takes a shard the first time it guards
  # a write to it — so at a fixed T+delay most pods own nothing yet. Picking a
  # pod blindly seizes zero rows and the scenario then "passes" without having
  # disrupted anything. Wait for a real owner, then target that one.
  for _ in $(seq 1 30); do
    victim=$(kc exec "$PGPOD" -- psql -U orcher -d orcher -tAc \
      "SELECT owner_node FROM cluster_shards WHERE owner_node IS NOT NULL
       GROUP BY owner_node ORDER BY count(*) DESC LIMIT 1;" 2>/dev/null | tr -d '[:space:]')
    [ -n "$victim" ] && break
    sleep 1
  done
  [ -n "$victim" ] || { log "  disrupt: no node owns a shard yet — nothing to seize"; return 0; }
  other=$(kc get pod -l app=orchestrator -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
    | grep -v "^${victim}$" | head -1)
  echo "$victim" > /tmp/orcher-seize-victim
  log "  disrupt: seize $victim's shards for $other (generation bump, victim not told)"
  # Seize repeatedly rather than once. A single bump is usually harmless: claim
  # scoping stops the superseded node from taking new work, so it reaches no
  # write for the fence to refuse. The fence guards the narrower case — a write
  # already in flight when ownership moves — so keep taking shards back while
  # the workload runs, to overlap a guard/commit window.
  local total=0 round
  for round in $(seq 1 "${SEIZE_ROUNDS:-20}"); do
    seized=$(kc exec "$PGPOD" -- psql -U orcher -d orcher -tAc \
      "WITH bumped AS (
         UPDATE cluster_shards SET owner_node = '$other', generation = generation + 1, updated_at = now()
         WHERE owner_node = '$victim' RETURNING 1)
       SELECT count(*) FROM bumped;" 2>/dev/null | tr -d '[:space:]')
    total=$((total + ${seized:-0}))
    # The victim reacquires whatever it still serves, so alternate the target to
    # keep generations moving under whichever node is currently writing.
    local tmp=$victim; victim=$other; other=$tmp
  done
  echo "$victim" > /tmp/orcher-seize-victim
  seized=$total
  log "  shards seized across ${SEIZE_ROUNDS:-20} rounds: ${seized:-0}"
  # A seizure of nothing tests nothing. Say so loudly rather than letting the
  # run report a green dup=0 that was never at risk.
  [ "${seized:-0}" -gt 0 ] || log "  WARNING: seized 0 shards — this run did not exercise the fence"
}

disrupt(){
  case "$SCENARIO" in
    kill)     disrupt_kill ;;
    scale-up) disrupt_scale_up ;;
    seize)    disrupt_seize ;;
    *)        die "unknown SCENARIO '$SCENARIO' (expected: kill | scale-up | seize)" ;;
  esac
}

# ---- one disrupt-and-assert scenario -----------------------------------------
# echoes: "dup=<n> completed=<x>/<y> stranded=<n>"
run_scenario(){
  # Always start from the configured replica count so repeated runs and the
  # scale-up scenario do not drift upward.
  kc scale deploy/orchestrator --replicas="$REPLICAS" >/dev/null 2>&1
  kc rollout status deploy/orchestrator --timeout=240s >/dev/null 2>&1
  kc wait --for=condition=ready pod -l app=orchestrator --timeout=120s >/dev/null 2>&1
  kc exec "$PGPOD" -- psql -U orcher -d orcher -tAc "TRUNCATE workflow_executions, workflow_execution_journal, outbox_queue, task_executions, execution_log, workflow_execution_logs, matching_partitions CASCADE;" >/dev/null 2>&1
  kc delete job stress --ignore-not-found >/dev/null 2>&1
  sed "s/__COUNT__/$COUNT/; s/__DEADLINE__/$DEADLINE/" "$HERE/manifests/40-stress-job.yaml" | kubectl --context "$KUBECTX" apply -f - >/dev/null 2>&1
  sleep "$DISRUPT_DELAY"
  disrupt & local disrupt_pid=$!
  local i ph
  for i in $(seq 1 $((DEADLINE / 5 + 15))); do
    ph=$(kc get pod -l app=stress -o jsonpath='{.items[0].status.phase}' 2>/dev/null)
    { [ "$ph" = "Succeeded" ] || [ "$ph" = "Failed" ]; } && break
    sleep 5
  done
  # Let the disruption finish before measuring, or the numbers below describe a
  # run that was never disrupted.
  wait "$disrupt_pid" 2>/dev/null || true
  # Did the run actually reach the shard-ownership machinery? A scenario that
  # passes without ever acquiring a shard has not tested ownership at all — it
  # has tested the SKIP LOCKED claim path, which is a different mechanism.
  #
  # This used to be legitimately 0, when fencing guarded only schedule_timer and
  # the stress workload never scheduled one. The fence now sits on the workflow
  # status writes (completion, failure, status update), which this workload does
  # perform, so a 0 here means something is wrong rather than "not applicable".
  local shards
  shards=$(kc exec "$PGPOD" -- psql -U orcher -d orcher -tAc \
    "SELECT count(*) FROM cluster_shards;" 2>/dev/null | tr -d '[:space:]')
  log "  shards acquired during run: ${shards:-?} (0 = ownership path not exercised)"

  # For the seize scenario, the number that matters is whether the superseded
  # node was actually STOPPED. dup=0 alone cannot distinguish "the fence refused
  # its writes" from "it happened to have no writes to make" — the same trap as
  # a claim filter that refuses everything looking identical to one that routes
  # correctly. Count the refusals it logged.
  if [ "$SCENARIO" = "seize" ]; then
    # Scan every orchestrator: the scenario alternates which node is superseded,
    # so refusals can land on any of them.
    local refusals=0 pod n
    for pod in $(kc get pod -l app=orchestrator -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
      n=$(kc logs "$pod" --tail=-1 2>/dev/null \
        | grep -cE "no longer holds its shard|ownership lost: held generation" || true)
      refusals=$((refusals + ${n:-0}))
    done
    if [ "$refusals" -gt 0 ]; then
      log "  fence refusals by superseded nodes: $refusals (mechanism engaged)"
    else
      log "  fence refusals by superseded nodes: 0 — the fence was never reached."
      log "    Not necessarily a failure: claim scoping removes a superseded node"
      log "    from work before it can write. dup=0 alone proves nothing here."
    fi
  fi

  local logs dup comp str
  logs=$(kc logs job/stress 2>&1)
  dup=$(echo "$logs" | grep -oE "duplicate executions : [0-9]+" | grep -oE "[0-9]+$"); dup=${dup:-0}
  comp=$(echo "$logs" | grep -oE "completed : [0-9]+/[0-9]+" | grep -oE "[0-9]+/[0-9]+" | head -1)
  str=$(echo "$logs" | grep -oE "stranded \(never ran\) : [0-9]+" | grep -oE "[0-9]+$"); str=${str:-0}
  echo "dup=$dup completed=$comp stranded=$str"
}

# ---- run RUNS scenarios ------------------------------------------------------
# What this gate asserts, and why the bar sits where it does:
#
#   * DUPLICATE EXECUTIONS -> hard failure. This is the property under test
#     (exactly-once through a pod kill) and it is measured from executions that
#     actually happened, so it is unaffected by client-side timeouts.
#
#   * COMPLETION RATE -> reported, with a floor (MIN_COMPLETION_PCT, default 90).
#     It is NOT a strict 100% check, because the load harness gives each workflow
#     a hardcoded 60s result wait (stress.rs) and then exits. On an undersized
#     runner the tail of the batch exceeds that wait, and the harness's own worker
#     pollers depart still holding claims on activations they polled but never
#     processed — those workflows show as incomplete/stranded even though the
#     engine had not failed. (Verified by inspecting the stuck rows: claimed_by =
#     the departed `...-wf-driver-poller-N`, claim_timeout_at still in the future.
#     In production another worker resumes them once the claim expires.)
#     Demanding 100% here would test the runner, not orcher. Set
#     MIN_COMPLETION_PCT=100 on a runner sized to sustain the full batch.
#   * For SCENARIO=scale-up the completion rate is the number to watch. The
#     newcomer computes ownership of its shards immediately, but acquire_shard
#     refuses while the prior owner is alive, and begin_guarded turns that
#     refusal into an error. Until each prior owner releases on its next
#     maintenance tick (~ORCHER_CLUSTER_HEARTBEAT_SECS), work routed to the
#     newcomer for those shards cannot proceed. Duplicates must still be zero —
#     safety is not in question here, handoff latency is.
MIN_COMPLETION_PCT=${MIN_COMPLETION_PCT:-90}
overall=0
for r in $(seq 1 "$RUNS"); do
  case "$SCENARIO" in
    kill)     what="$KILL_MODE kill" ;;
    scale-up) what="scale $REPLICAS->$((REPLICAS + 1))" ;;
    seize)    what="shard seizure from a live node" ;;
  esac
  log "[$SCENARIO] run $r/$RUNS — $COUNT workflows, $what at T+${DISRUPT_DELAY}s"
  res=$(run_scenario); echo "  → $res"
  dup=$(echo "$res" | grep -oE "dup=[0-9]+" | cut -d= -f2); dup=${dup:-0}
  comp=$(echo "$res" | grep -oE "completed=[0-9]+/[0-9]+" | cut -d= -f2)
  done_n=${comp%/*}; total_n=${comp#*/}
  if [ "$dup" -gt 0 ]; then
    echo "  ❌ EXACTLY-ONCE VIOLATED — $dup duplicate execution(s). Hard fail."
    overall=1; break
  fi
  pct=$(( ${done_n:-0} * 100 / ${total_n:-1} ))
  if [ "$pct" -lt "$MIN_COMPLETION_PCT" ]; then
    echo "  ❌ completion ${pct}% (${comp}) below floor ${MIN_COMPLETION_PCT}% — runner too slow, or a real liveness regression"
    overall=1; break
  fi
  echo "  ✅ run $r passed — 0 duplicates, ${comp} completed (${pct}%)"
done

log "VERDICT"
case "$SCENARIO" in
  kill)     survived="$RUNS $KILL_MODE pod kill(s)" ;;
  scale-up) survived="$RUNS scale-up rebalance(s)" ;;
esac
if [ "$overall" = 0 ]; then echo "✅ [$SCENARIO] PASS — exactly-once survived $survived on Kubernetes"
else echo "❌ [$SCENARIO] FAIL — kubectl -n $NS logs job/stress"; fi
echo; echo "Cluster left running (context k3d-$CLUSTER; inspect with 'k9s'). Tear down: $0 --cleanup"
exit "$overall"
