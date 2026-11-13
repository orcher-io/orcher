#!/usr/bin/env bash
#
# The CI gate, and the only definition of it.
#
# .github/workflows/ci.yml runs this on a GitHub-hosted runner, and so can
# anyone on their own machine before pushing:
#
#   scripts/ci.sh               # everything CI runs
#   scripts/ci.sh check         # fmt, warning-free compile, library tests
#   scripts/ci.sh guards        # the database-backed guards only
#   scripts/ci.sh contract      # real workflows and the tenancy checks
#
# The commands live here rather than in the workflow so that "passes CI" and
# "passes locally" cannot drift apart: when hosted minutes run out, a run of
# this script is the same gate, not an approximation of it.
#
# The contract stage needs the sdk-rust repository beside this one, or at
# CONTRACT_SDK_RUST_DIR.
#
# Database: with CI_DATABASE_URL set, the guards use that database as-is (the
# workflow starts a throwaway Postgres on hosted runners). Otherwise a fresh
# database is created on the local server (CI_POSTGRES_ADMIN_URL, default the
# current user's localhost) and dropped when the script exits.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

export SQLX_OFFLINE=true
export CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}"
# One set of flags for every step, so the compile check, the unit tests and
# the guards share build artifacts instead of each rebuilding the workspace.
export RUSTFLAGS="${RUSTFLAGS:--D warnings}"

step() { printf '\n==> %s\n' "$*"; }

run_check() {
  step "format"
  cargo fmt --all --check
  # A statement that selects or returns * is refused by Postgres on every
  # connection that prepared it, once a migration adds a column to its table:
  # in a rolling upgrade, every claim the old build makes.
  step "SQL names its columns"
  bash scripts/check-sql-columns.sh
  step "compile (offline, all targets, deny warnings)"
  cargo check --workspace --all-targets --locked
  # Clippy's default lints, as errors. Named among them, the lints for a lock
  # held across an .await, which compile cleanly and hang a runtime thread
  # under load; clippy.toml adds DashMap's guards to them. Code inside macro
  # expansions (async_trait service methods among them) is outside clippy's
  # reach, which is why the engine's completion notifier keeps its map private
  # instead of trusting this alone.
  step "clippy (deny warnings)"
  cargo clippy --workspace --all-targets --locked -- -D warnings \
    -D clippy::await_holding_lock -D clippy::await_holding_refcell_ref \
    -D clippy::await_holding_invalid_type
  # Single-threaded: a handful of unit tests mutate process environment
  # variables and race under the default parallel runner.
  step "library unit tests"
  cargo test --workspace --lib --locked -- --test-threads=1
}

# The database-backed guards.
#
# Most of this repository's `#[ignore]`d tests want a schema and a fixture this
# gate does not build, so it runs a named set rather than everything: the
# checks that stand behind fixes whose failure mode is silent — one tenant
# reaching another's data, a worker quietly going deaf, an event dropped on a
# race. Those are exactly the tests nobody runs by accident.
#
# Add to the list when you add a guard. A test that never runs is a comment.
GUARDS=(
  "Tenancy — a caller sees only its own workers|--lib workers::tests::tenancy -- --ignored --test-threads=1"
  "Tenancy — a schedule name belongs to an organisation|--lib schedule::repository::tenancy -- --ignored --test-threads=1"
  "Tenancy — a batch acts on one organisation's work|--test batch_status_skips_terminal -- --ignored --test-threads=1"
  "Lifecycle — worker health and the silent-worker sweep|--lib worker::repository -- --ignored --test-threads=1"
  "Journal — concurrent appends all land, a repeated outcome lands once|--lib journal::repository -- --ignored --test-threads=1"
  "Children — a refused start fails the parent, a half-recorded start converges|--test child_start -- --ignored --test-threads=1"
  "Children — a child's outcome reaches its real parent once|--test child_lifecycle -- --ignored --test-threads=1"
  "Sessions — a dead session's tasks are failed and their workflows woken|--lib retries::timeout_watcher -- --ignored --test-threads=1"
  "Tenancy — an event by workflow id reaches only the caller's own workflow (lookup)|--lib a_workflow_id_resolves_to_the_callers_own_workflow -- --ignored"
  "Tenancy — an event by workflow id reaches only the caller's own workflow (delivery)|--test event_tenancy -- --ignored --test-threads=1"
  "Runs — a request names one run the same way in every RPC, scoped to its namespace and organization (lookup)|--lib a_request_names_the_run_the_caller_may_see -- --ignored"
  "Runs — describe, status, result, journal, logs, events, queries, updates, cancel, reset and terminate name the caller's own run (RPCs)|--test run_resolution -- --ignored --test-threads=1"
  "Sharding — a shard's fence is readable on the database its rows live in|--lib an_acquired_generation_fences -- --ignored"
  "Sharding — a completion that reaches a non-owner commits on the owner|--test forwarded_completion_commits_on_owner -- --ignored --test-threads=1"
  "Sharding — a failed run is retried by the node that owns it|--test retry_left_to_owner -- --ignored --test-threads=1"
  "Tasks — a failure is recorded whole and once, and a cancellation at all|--test task_outcome -- --ignored --test-threads=1"
  "Tasks — a report is answered once, and only for the attempt still running|--test task_answers -- --ignored --test-threads=1"
  "Tasks — a cancellation stops queued work too: not started, ended with a failure the workflow replays, never retried, while cleanup runs|--test cancel_queued_tasks -- --ignored --test-threads=1"
  "Tasks — a cancelled task wakes its workflow and replays as a Cancelled failure, however it ended; a scheduled task can be cancelled|--test cancelled_task_steps -- --ignored --test-threads=1"
  "Replicas — a heartbeat reaches a replica the worker did not register with|--lib a_heartbeat_reaches_a_replica_the_worker_did_not_register_with -- --ignored"
  "Completions — applied once, and only for the activation they answer|--test activation_fence -- --ignored --test-threads=1"
  "Completions — many at once finish on a small connection pool|--test completions_under_pool_pressure -- --ignored --test-threads=1"
  "Tasks — a scheduled task is dispatched once, even when its engine stopped first|--test undispatched_task -- --ignored --test-threads=1"
  "Cluster — a dead engine's outstanding claims are released by its survivors, once|--test dead_node_claims -- --ignored --test-threads=1"
  "Workers — a dead worker process's activation reaches another worker within seconds, its late completion is not applied, per organization|--test lapsed_worker_claims -- --ignored --test-threads=1"
  "Workers — a stopping worker hands back what it will not run and is given nothing new, on every replica; a worker keeping a run's journal is sent only its new entries, and the whole journal on any doubt|--test worker_lifecycle -- --ignored --test-threads=1"
  "Tenancy — a worker is handed only its own organization's workflows and tasks|--test claim_tenancy -- --ignored --test-threads=1"
  "Tenancy — actor state belongs to the organization that wrote it|--test actor_state_tenancy -- --ignored --test-threads=1"
  "Liveness — an eager task is not run twice, a lost wake-up is recovered, matching rows do not pile up|--test liveness_sweeps -- --ignored --test-threads=1"
  "Liveness — a task whose worker stopped answering is retried after the default heartbeat timeout, an old worker's long task is not|--test auto_heartbeat -- --ignored --test-threads=1"
  "Startup — a shard database behind this build refuses the start|--lib a_shard_behind_this_build_refuses_the_start -- --ignored"
  "Starts — a workflow id is started once while open, per organization, as its reuse policy allows|--test workflow_id_reuse -- --ignored --test-threads=1"
  "Dispatch — a new run wakes a waiting workflow poll|--test start_wakes_polls -- --ignored --test-threads=1"
  "Dispatch — a workflow poll that may not wait still claims what is ready|--test poll_without_waiting -- --ignored --test-threads=1"
  "Heartbeats — the heartbeats of several running tasks are found and recorded together|--test batched_heartbeats -- --ignored --test-threads=1"
  "Timeouts — a run timeout ends the run and is retried by a fresh run, the execution timeout bounds every run and is never retried, an unanswered activation is handed on after its task timeout|--test start_timeouts -- --ignored --test-threads=1"
  "Events — an event one workflow sends another is replayed by the target once, on any shard|--test workflow_sent_event -- --ignored --test-threads=1"
  "Events — an event a client sends with a request id is delivered once per run, however often or concurrently the request is repeated, by SendEvent and by an event-with-start finding the workflow running; another request id, or none, is another event|--test event_delivery_once -- --ignored --test-threads=1"
  "Upgrades — a column added under a live pool breaks no claim and no journal read or write|--test additive_migration -- --ignored --test-threads=1"
  "Upgrades — a migrated database, new or upgraded, holds none of the tables and functions of the features the engine never had or never used (alerts, notifications, plugins, integrations, a message queue, an event store)|--test dropped_tables -- --ignored --test-threads=1"
  "Failures — a run that fails with an error longer than a notification can carry is still recorded as failed|--test long_failure_error -- --ignored --test-threads=1"
  "Tenancy — an organization id is a key the engine never looks up: no engine table references the account tables, and a run or key is stored for an organization they do not hold|--test opaque_organizations -- --ignored --test-threads=1"
  "Upgrades — a statement prepared against an older schema is cleared and retried, its connection recycled|--lib stale_plan -- --ignored --test-threads=1"
  "Payloads — a task result travels once, one over the limit fails what produced it, an activation too large for its worker fails the workflow rather than looping|--test payload_limits -- --ignored --test-threads=1"
  "Payloads — the gRPC server takes messages over 4 MiB and refuses a payload over the limit by name|--bin orchestrator the_grpc_server_takes_large_messages -- --test-threads=1"
  "Tasks — a first attempt is handed out as attempt 1 and its first retry as 2, eagerly, from the outbox, a session's queue or a matching partition; maximum_attempts counts the first attempt and 0 is unlimited|--test task_attempt_numbering -- --ignored --test-threads=1"
  "Runs — a first run's activations are attempt 1, replayed or not, and its workflow-level retry's are attempt 2|--test workflow_attempt_numbering -- --ignored --test-threads=1"
  "Retries — maximum_attempts counts the first run (0 unlimited), a result waiter gets the final outcome not a failure about to be retried, a short backoff retries promptly|--test workflow_retry_semantics -- --ignored --test-threads=1"
  "Retries — a workflow failure raised as non-retryable is not retried, reported by command or call, and the retry scheduler agrees; any other failure is retried by the policy; an older worker's mark is not honoured|--test workflow_non_retryable_failure -- --ignored --test-threads=1"
  "Starts — a batch and an event-with-start store every setting a single start does, column for column; a batch takes each id under its reuse policy and each queue as its own; a child keeps its own time limits; describe reports the run's attempt and annotations; a delayed run is stored alike by every path, handed out by no claim before it is due, released when due, and its clocks start then; a repeated request id is answered with the run it made by single, batch and event-with-start, racing repeats make one run|--test start_paths -- --ignored --test-threads=1"
  "Logs — a run's log reads in write order, follows by id, and says the run started once across its activations|--test execution_log_order -- --ignored --test-threads=1"
  "Dispatch — a poll takes its own queue's oldest task, once, never held back by another queue's; a task handed out leaves nothing to claim until it is put back|--test claim_per_queue -- --ignored --test-threads=1"
  "Outbox — the processor claims the oldest ready rows, several of one workflow at once, never held back by a row another claimer holds or one in backoff|--test outbox_claim_oldest -- --ignored --test-threads=1"
  "Sharding — a node claims only outbox rows on the shards it owns, rows from before shards stay claimable, the sweep picks up a row no one owns, no scope claims everything|--test shard_filtered_claims -- --ignored --test-threads=1"
)

CREATED_DB=""
drop_created_database() {
  [ -n "$CREATED_DB" ] || return 0
  for db in "$CREATED_DB" "${CREATED_DB}_fence_data" "${CREATED_DB}_undispatched_shard" \
    "${CREATED_DB}_misrouted_shard" "${CREATED_DB}_deadclaims_shard" "${CREATED_DB}_behind_shard" \
    "${CREATED_DB}_heartbeat_shard" "${CREATED_DB}_sentevent_shard" "${CREATED_DB}_lapsed_shard"; do
    psql "$ADMIN_URL" -qtAc "DROP DATABASE IF EXISTS \"$db\" WITH (FORCE)" >/dev/null 2>&1 || true
  done
}

ensure_database() {
  [ -z "${DB_URL:-}" ] || return 0
  if [ -n "${CI_DATABASE_URL:-}" ]; then
    DB_URL="$CI_DATABASE_URL"
  else
    ADMIN_URL="${CI_POSTGRES_ADMIN_URL:-postgresql://$(whoami)@localhost:5432/postgres}"
    CREATED_DB="orcher_ci_${GITHUB_RUN_ID:-local}_${GITHUB_RUN_ATTEMPT:-$$}"
    trap drop_created_database EXIT
    drop_created_database
    step "fresh database $CREATED_DB"
    psql "$ADMIN_URL" -qtAc "CREATE DATABASE \"$CREATED_DB\"" >/dev/null
    DB_URL="${ADMIN_URL%/*}/$CREATED_DB"
  fi
  export DATABASE_URL="$DB_URL" TEST_DATABASE_URL="$DB_URL"

  step "create the schema"
  cargo run --locked -p orcher-storage --bin orcher-migrate -- run
}

run_guards() {
  ensure_database

  # --workspace on every guard, as on the unit tests: selecting one package
  # with -p changes which features cargo unifies, so each guard used to
  # recompile shared crates — minutes per guard. Each guard's --lib/--test
  # target and name filter still select exactly the tests they did.
  local failed=0
  for guard in "${GUARDS[@]}"; do
    step "${guard%%|*}"
    # shellcheck disable=SC2086 # the argument list is split on purpose
    cargo test --locked --workspace ${guard#*|} </dev/null || failed=1
  done
  return "$failed"
}

# Real workflows against a real engine, then the tenancy checks against one
# in api-key mode — see scripts/contract-suite.sh. The only check here that
# a change still lets the engine orchestrate anything.
run_contract() {
  ensure_database
  step "contract suite and tenancy checks"
  VERIFY_DATABASE_URL="$DB_URL" bash scripts/contract-suite.sh
}

case "${1:-all}" in
  check) run_check ;;
  guards) run_guards ;;
  contract) run_contract ;;
  all) run_check && run_guards && run_contract ;;
  *)
    echo "usage: scripts/ci.sh [all|check|guards|contract]" >&2
    exit 2
    ;;
esac
