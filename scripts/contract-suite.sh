#!/usr/bin/env bash
#
# Run the cross-SDK contract suite against a throwaway engine.
#
# The suite is the only check that a change still lets the engine orchestrate
# anything: everything else in `verify.sh` proves the code compiles and that
# units behave, which has never been the thing that broke. Every durability bug
# in this project's history — a child created in the wrong namespace, a permit
# released under the wrong identity, a retry sweep that starved itself — passed
# the type checker and the unit tests and was found by a real worker running
# real workflows.
#
# Until now that required a person to remember four steps in order: start
# Postgres, migrate, start an orchestrator, create the namespace. A check with a
# four-step preamble is a check that gets skipped under exactly the time
# pressure that makes it worth running, so this does the preamble itself and
# cleans up after.
#
# Ports and the database are deliberately not the defaults: running this must
# never disturb an engine someone is already using on 50051.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Where cargo put the binaries: CARGO_TARGET_DIR, if the build is kept outside the checkout.
BIN_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}/debug"
# Beside this repository by default; CI checks it out inside the workspace.
SDK_RUST="$(cd "${CONTRACT_SDK_RUST_DIR:-$REPO_ROOT/../sdk-rust}" 2>/dev/null && pwd || true)"

GRPC_PORT="${CONTRACT_GRPC_PORT:-50251}"
HTTP_PORT="${CONTRACT_HTTP_PORT:-8201}"
NAMESPACE="${CONTRACT_NAMESPACE:-contract}"
: "${VERIFY_DATABASE_URL:=postgresql://${USER}@localhost:5432/orcher_verify}"
LOG="${CONTRACT_ENGINE_LOG:-/tmp/orcher-contract-engine.log}"

if [ -z "$SDK_RUST" ] || [ ! -d "$SDK_RUST/contract" ]; then
  echo "contract-suite: sdk-rust/contract not found beside this repo — nothing to run." >&2
  echo "contract-suite: expected at $(dirname "$REPO_ROOT")/sdk-rust/contract" >&2
  exit 2
fi

ENGINE_PID=""
cleanup() {
  if [ -n "$ENGINE_PID" ] && kill -0 "$ENGINE_PID" 2>/dev/null; then
    kill "$ENGINE_PID" 2>/dev/null || true
    wait "$ENGINE_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "contract-suite: building the engine"
cargo build -q --bin orchestrator --bin orcher-migrate || exit 1

echo "contract-suite: applying migrations to ${VERIFY_DATABASE_URL##*/}"
DATABASE_URL="$VERIFY_DATABASE_URL" cargo run -q --bin orcher-migrate -- run >/dev/null || exit 1

# The ports must be free. If an engine left over from an earlier run holds
# them, the new one cannot bind, and the health check below is answered by the
# old one — so the suite passes against a binary that is not this build.
for port in "$GRPC_PORT" "$HTTP_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "contract-suite: port $port is already in use — stop whatever holds it, or set CONTRACT_GRPC_PORT / CONTRACT_HTTP_PORT." >&2
    exit 1
  fi
done

echo "contract-suite: starting engine on :$GRPC_PORT (log: $LOG)"
# A task nothing heartbeats is timed out within seconds rather than a minute,
# so the worker-liveness scenarios — a task outliving its heartbeat timeout
# on the worker's own heartbeats, a task retried after its worker dies — run
# in the suite's time and can fail.
ORCHER_DEFAULT_HEARTBEAT_TIMEOUT_SECS=3 \
ORCHER_TIMEOUT_WATCHER_POLL_INTERVAL_SECS=1 \
ORCHER_TIMEOUT_WATCHER_GRACE_SECS=1 \
DATABASE_URL="$VERIFY_DATABASE_URL" \
GRPC_BIND_ADDR="127.0.0.1:$GRPC_PORT" \
HTTP_BIND_ADDR="127.0.0.1:$HTTP_PORT" \
LOG_LEVEL="${CONTRACT_ENGINE_LOG_LEVEL:-warn}" \
LOG_FORMAT=compact \
  "$BIN_DIR/orchestrator" >"$LOG" 2>&1 &
ENGINE_PID=$!

# Wait on the health endpoint rather than a fixed sleep: a sleep that is long
# enough on a warm machine is not long enough on a cold one, and the failure
# looks like a broken engine rather than an impatient script.
# Liveness first: a health answer only counts if our engine is still running.
for _ in $(seq 1 60); do
  if ! kill -0 "$ENGINE_PID" 2>/dev/null; then
    echo "contract-suite: engine exited during startup — last lines of $LOG:" >&2
    tail -20 "$LOG" >&2
    exit 1
  fi
  if curl -fsS -m 2 "http://127.0.0.1:$HTTP_PORT/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! kill -0 "$ENGINE_PID" 2>/dev/null \
  || ! curl -fsS -m 2 "http://127.0.0.1:$HTTP_PORT/health" >/dev/null 2>&1; then
  echo "contract-suite: engine never became healthy — last lines of $LOG:" >&2
  tail -20 "$LOG" >&2
  exit 1
fi

# The suite runs in a named namespace rather than `default`, where a
# child-workflow tenancy bug hid for months: children were created in a
# namespace literally called "default" instead of their parent's, and in
# `default` the wrong answer and the right one are the same string. The
# harness creates it if it is missing, so the suite needs no CLI.

echo "contract-suite: running scenarios in namespace '$NAMESPACE'"
(
  cd "$SDK_RUST" && \
  RUSTFLAGS="${CONTRACT_SDK_RUSTFLAGS:-}" \
  ORCHER_SERVER_URL="http://127.0.0.1:$GRPC_PORT" \
  ORCHER_NAMESPACE="$NAMESPACE" \
    ./contract/run.sh
)
SUITE_EXIT=$?

if [ "$SUITE_EXIT" -ne 0 ]; then
  echo "contract-suite: scenarios failed — engine log at $LOG" >&2
fi

# A passing suite is not enough if the engine panicked on the way. For months
# the workflow-completion RPC panicked on every completion wherever the
# Prometheus recorder was installed; workflows still finished, every scenario
# passed, and the only trace was a worker-side "h2 protocol error" that read as
# network noise. A panic in any engine task fails the run.
if grep -q "panicked at" "$LOG"; then
  echo "contract-suite: the engine panicked during the run — scenarios passing does not make that fine:" >&2
  grep -A 2 "panicked at" "$LOG" | head -12 >&2
  exit 1
fi

[ "$SUITE_EXIT" -eq 0 ] || exit "$SUITE_EXIT"

# ── Tenancy ─────────────────────────────────────────────────────────────────
#
# The scenarios above run with no credentials, so every tenancy guard in the
# engine is a no-op for them and a regression in any of them would pass. This
# runs a second engine in api-key mode on the same database, gives it two
# organisations and four keys of its own — one per organisation, one bound to
# none, and one allowed to act for any (the shape of the gateway's) — and has
# the tenancy harness check that one organisation cannot see or change the
# other's work. Everything it creates is removed afterwards.
T_GRPC_PORT="${CONTRACT_TENANCY_GRPC_PORT:-$((GRPC_PORT + 1))}"
T_HTTP_PORT="${CONTRACT_TENANCY_HTTP_PORT:-$((HTTP_PORT + 1))}"
T_LOG="${CONTRACT_TENANCY_ENGINE_LOG:-/tmp/orcher-contract-tenancy-engine.log}"
# A directory, so the file can end in .toml: BSD `mktemp -t` takes a bare
# prefix but GNU's wants trailing X's, and without them the config landed in
# ./.toml on Linux runners.
T_DIR="$(mktemp -d "${TMPDIR:-/tmp}/orcher-tenancy.XXXXXX")" || {
  echo "contract-suite: cannot create a temporary directory" >&2
  exit 1
}
T_CONFIG="$T_DIR/config.toml"
for port in "$T_GRPC_PORT" "$T_HTTP_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "contract-suite: port $port is already in use — set CONTRACT_TENANCY_GRPC_PORT / CONTRACT_TENANCY_HTTP_PORT." >&2
    exit 1
  fi
done

sql() { psql "$VERIFY_DATABASE_URL" -v ON_ERROR_STOP=1 -qtAc "$1"; }
new_key() { printf 'orch_ct_%s' "$(openssl rand -hex 24)"; }
key_hash() { printf '%s' "$1" | shasum -a 256 | cut -d' ' -f1; }

T_USER=$(uuidgen | tr 'A-Z' 'a-z'); T_ORG_A=$(uuidgen | tr 'A-Z' 'a-z'); T_ORG_B=$(uuidgen | tr 'A-Z' 'a-z')
KEY_A=$(new_key); KEY_B=$(new_key); KEY_UNBOUND=$(new_key); KEY_DELEGATE=$(new_key)
T_ENGINE_PID=""
tenancy_cleanup() {
  if [ -n "$T_ENGINE_PID" ] && kill -0 "$T_ENGINE_PID" 2>/dev/null; then
    kill "$T_ENGINE_PID" 2>/dev/null || true
    wait "$T_ENGINE_PID" 2>/dev/null || true
  fi
  sql "DELETE FROM api_keys WHERE user_id = '$T_USER';
       DELETE FROM organizations WHERE id IN ('$T_ORG_A', '$T_ORG_B');
       DELETE FROM users WHERE id = '$T_USER';" >/dev/null 2>&1 || true
  rm -rf "$T_DIR"
  cleanup
}
trap tenancy_cleanup EXIT INT TERM

sql "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at)
       VALUES ('$T_USER', 'contract-$T_USER@tenancy.test', 'x', 'argon2', NOW());
     INSERT INTO organizations (id, name, display_name, slug, owner_id) VALUES
       ('$T_ORG_A', 'contract-a-$T_ORG_A', 'contract-a', 'contract-a-$T_ORG_A', '$T_USER'),
       ('$T_ORG_B', 'contract-b-$T_ORG_B', 'contract-b', 'contract-b-$T_ORG_B', '$T_USER');
     INSERT INTO api_keys (user_id, organization_id, name, key_prefix, key_hash, can_delegate_organization) VALUES
       ('$T_USER', '$T_ORG_A', 'contract a',        'orch_ct_', '$(key_hash "$KEY_A")',        false),
       ('$T_USER', '$T_ORG_B', 'contract b',        'orch_ct_', '$(key_hash "$KEY_B")',        false),
       ('$T_USER', NULL,       'contract unbound',  'orch_ct_', '$(key_hash "$KEY_UNBOUND")',  false),
       ('$T_USER', NULL,       'contract delegate', 'orch_ct_', '$(key_hash "$KEY_DELEGATE")', true);" \
  || { echo "contract-suite: could not create the tenancy fixture" >&2; exit 1; }

cat > "$T_CONFIG" <<TOML
[server]
grpc_addr = "127.0.0.1:$T_GRPC_PORT"
http_addr = "127.0.0.1:$T_HTTP_PORT"
max_concurrent_streams = 1000
[orchestrator]
max_workers = 100
max_concurrent_tasks = 1000
heartbeat_timeout_secs = 90
[storage]
database_url = "$VERIFY_DATABASE_URL"
[logging]
level = "${CONTRACT_ENGINE_LOG_LEVEL:-warn}"
format = "compact"
[health]
readiness_enabled = true
liveness_enabled = true
[polling]
[timeout_watcher]
[workflow_retry]
[background_jobs]
[security]
auth_mode = "api-key"
# Organization-bound keys: a multi-tenant deployment, which must say so.
tenancy = "multi"
TOML

echo "contract-suite: starting an api-key engine on :$T_GRPC_PORT for the tenancy checks (log: $T_LOG)"
DATABASE_URL="$VERIFY_DATABASE_URL" \
  "$BIN_DIR/orchestrator" --config "$T_CONFIG" >"$T_LOG" 2>&1 &
T_ENGINE_PID=$!
for _ in $(seq 1 60); do
  kill -0 "$T_ENGINE_PID" 2>/dev/null || { echo "contract-suite: the api-key engine exited — last lines of $T_LOG:" >&2; tail -20 "$T_LOG" >&2; exit 1; }
  curl -fsS -m 2 "http://127.0.0.1:$T_HTTP_PORT/health" >/dev/null 2>&1 && break
  sleep 1
done

echo "contract-suite: running the tenancy checks"
(
  cd "$SDK_RUST" && \
  RUSTFLAGS="${CONTRACT_SDK_RUSTFLAGS:-}" \
  ORCHER_SERVER_URL="http://127.0.0.1:$T_GRPC_PORT" \
  ORCHER_TENANCY_KEY_A="$KEY_A" ORCHER_TENANCY_ORG_A="$T_ORG_A" \
  ORCHER_TENANCY_KEY_B="$KEY_B" ORCHER_TENANCY_ORG_B="$T_ORG_B" \
  ORCHER_TENANCY_KEY_UNBOUND="$KEY_UNBOUND" ORCHER_TENANCY_KEY_DELEGATE="$KEY_DELEGATE" \
    cargo run -q -p orcher-contract --bin tenancy
)
TENANCY_EXIT=$?
[ "$TENANCY_EXIT" -eq 0 ] || echo "contract-suite: tenancy checks failed — engine log at $T_LOG" >&2

if grep -q "panicked at" "$T_LOG"; then
  echo "contract-suite: the api-key engine panicked during the tenancy checks:" >&2
  grep -A 2 "panicked at" "$T_LOG" | head -12 >&2
  exit 1
fi

exit "$TENANCY_EXIT"
