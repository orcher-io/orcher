#!/usr/bin/env bash
#
# One command, one answer: does this tree work?
#
# Exists because a check that cannot fail is worse than no check. Verification
# here was a sequence of remembered commands whose output got skimmed, and that
# failed exactly the way it sounds like it would: a run of `cargo check` was
# reported clean when `cargo` was not on PATH at all — the shell's error went to
# stderr while the reader watched stdout, so "no errors printed" and "nothing
# ran" looked identical. Every stage below is checked by exit status, the
# toolchain is resolved before anything else, and the summary at the end
# distinguishes passed from skipped.
#
# Usage:
#   scripts/verify.sh                 static checks and unit tests
#   scripts/verify.sh --with-db       also the database-backed suites
#   scripts/verify.sh --with-contract also a real workflow run against an engine
#   scripts/verify.sh --all           everything
#
# --with-contract boots a throwaway engine on a scratch database and runs the
# cross-SDK contract suite against it. That is the only stage that proves the
# engine still runs workflows; the others prove it still compiles.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

WITH_DB=0
WITH_CONTRACT=0
for arg in "$@"; do
  case "$arg" in
    --with-db) WITH_DB=1 ;;
    --with-contract) WITH_CONTRACT=1 ;;
    --all) WITH_DB=1; WITH_CONTRACT=1 ;;
    -h|--help) sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $arg (try --help)" >&2; exit 2 ;;
  esac
done

# ---------------------------------------------------------------------------
# Toolchain. Resolved once, explicitly, before any stage runs: a missing cargo
# must stop the script rather than turn every stage into a silent no-op.
# ---------------------------------------------------------------------------
if ! command -v cargo >/dev/null 2>&1; then
  for candidate in "$HOME/.cargo/bin" "$HOME"/.rustup/toolchains/*/bin; do
    if [ -x "$candidate/cargo" ]; then
      PATH="$candidate:$PATH"
      break
    fi
  done
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "verify: cargo is not on PATH and none was found under ~/.cargo or ~/.rustup." >&2
  echo "verify: refusing to run — every stage would report success without executing." >&2
  exit 127
fi

export PATH
echo "toolchain: $(cargo --version) at $(command -v cargo)"
echo

# ---------------------------------------------------------------------------
# Stage plumbing. Each stage records pass/fail/skip and the run continues, so
# one failure does not hide the state of everything after it.
# ---------------------------------------------------------------------------
STAGE_NAMES=()
STAGE_RESULTS=()
FAILED=0

run_stage() {
  local name="$1"; shift
  printf '\033[1m==> %s\033[0m\n' "$name"
  if "$@"; then
    STAGE_NAMES+=("$name"); STAGE_RESULTS+=("pass")
    printf '\033[32m    ok\033[0m\n\n'
  else
    local code=$?
    STAGE_NAMES+=("$name"); STAGE_RESULTS+=("FAIL")
    FAILED=1
    printf '\033[31m    failed (exit %d)\033[0m\n\n' "$code"
  fi
}

skip_stage() {
  STAGE_NAMES+=("$1"); STAGE_RESULTS+=("skip: $2")
  printf '\033[1m==> %s\033[0m\n\033[33m    skipped — %s\033[0m\n\n' "$1" "$2"
}

# ---------------------------------------------------------------------------
# Static checks and unit tests.
# ---------------------------------------------------------------------------
run_stage "formatting" cargo fmt --all -- --check
run_stage "compile (workspace, all targets)" cargo check --workspace --all-targets
run_stage "unit tests" cargo test --workspace --lib

# Clippy is reported but does not gate: the workspace carries pre-existing lints
# that predate this script, and failing on them would mean every run is red for
# reasons unrelated to the change in hand. Gate it here once that backlog is
# clear — a lint that never blocks anything is one nobody fixes.
printf '\033[1m==> clippy (advisory)\033[0m\n'
CLIPPY_COUNT="$(cargo clippy --workspace --all-targets 2>&1 | grep -cE '^(error|warning)(\[|:)' || true)"
printf '    %s diagnostic(s); not gating\n\n' "$CLIPPY_COUNT"
STAGE_NAMES+=("clippy (advisory)"); STAGE_RESULTS+=("$CLIPPY_COUNT diagnostics, not gating")

# ---------------------------------------------------------------------------
# Database-backed suites. These are #[ignore]d so an ordinary `cargo test` does
# not fail on a machine with no Postgres; they are the ones that check the SQL.
# ---------------------------------------------------------------------------
: "${VERIFY_DATABASE_URL:=postgresql://${USER}@localhost:5432/orcher_verify}"

database_reachable() {
  command -v psql >/dev/null 2>&1 && psql "$VERIFY_DATABASE_URL" -c 'SELECT 1' >/dev/null 2>&1
}

prepare_database() {
  psql "${VERIFY_DATABASE_URL%/*}/postgres" -q \
    -c "DROP DATABASE IF EXISTS $(basename "$VERIFY_DATABASE_URL") WITH (FORCE);" \
    -c "CREATE DATABASE $(basename "$VERIFY_DATABASE_URL");" >/dev/null 2>&1 \
    && cargo run -q --bin orcher-migrate -- run >/dev/null
}

if [ "$WITH_DB" = 1 ] || [ "$WITH_CONTRACT" = 1 ]; then
  if command -v psql >/dev/null 2>&1 && psql "${VERIFY_DATABASE_URL%/*}/postgres" -c 'SELECT 1' >/dev/null 2>&1; then
    export DATABASE_URL="$VERIFY_DATABASE_URL"
    run_stage "database: fresh schema" prepare_database
  else
    skip_stage "database: fresh schema" "no Postgres reachable at ${VERIFY_DATABASE_URL%%\?*}"
    WITH_DB=0; WITH_CONTRACT=0
  fi
fi

if [ "$WITH_DB" = 1 ]; then
  run_stage "database-backed suites" cargo test --workspace -- --ignored
fi

# ---------------------------------------------------------------------------
# A real workflow, against a real engine. The only stage that can tell you the
# engine still orchestrates anything rather than merely still compiling.
# ---------------------------------------------------------------------------
if [ "$WITH_CONTRACT" = 1 ]; then
  run_stage "contract suite (live engine)" "$REPO_ROOT/scripts/contract-suite.sh"
fi

# ---------------------------------------------------------------------------
printf '\033[1m==> summary\033[0m\n'
for i in "${!STAGE_NAMES[@]}"; do
  printf '    %-38s %s\n' "${STAGE_NAMES[$i]}" "${STAGE_RESULTS[$i]}"
done
echo

if [ "$FAILED" = 1 ]; then
  printf '\033[31mverify: FAILED\033[0m\n'
  exit 1
fi
printf '\033[32mverify: ok\033[0m\n'
