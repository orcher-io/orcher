#!/usr/bin/env bash
# Note: This script is compatible with bash 3.x (macOS default)

#==============================================================================
# ORCHER Unified Performance Benchmark
#==============================================================================
#
# This script provides comprehensive performance benchmarking for ORCHER,
# combining the best features of benchmark_multiworker.sh and benchmark_sharding.sh.
#
# Key improvements:
# - Uses consistent namespace/queue to avoid orchestrator state confusion
# - Properly waits for orchestrator state cleanup between tests
# - Supports Redis Streams and Sync-Match modes
# - Accurate scaling efficiency measurement
# - TypeScript SDK support
# - Auto-scaling mode: workflows scale proportionally with worker count
#
# Usage:
#   ./scripts/benchmark.sh                              # Default: Sync-Match mode, 1-4 workers
#   ./scripts/benchmark.sh --workers 2,4,8              # Test specific worker counts
#   ./scripts/benchmark.sh --workflows 50               # 50 concurrent workflows per worker
#   ./scripts/benchmark.sh --workflows-total 200        # 200 total workflows (fixed)
#   ./scripts/benchmark.sh --auto-scale                 # Scale workflows with workers (recommended)
#   ./scripts/benchmark.sh --sdk rust                   # Test Rust SDK only
#   ./scripts/benchmark.sh --sdk typescript             # Test TypeScript SDK only
#   ./scripts/benchmark.sh --quick                      # Quick test (1,2 workers, 10 workflows)
#
# IMPORTANT: Start orchestrator with benchmark config:
#   cargo run --release --bin orchestrator -- --config services/orchestrator/config/benchmark.toml
#
# The benchmark.toml config disables schedule-to-start timeouts which cause
# cascading failures under high contention. See docs/SINGLE_INSTANCE_OPTIMIZATION_ROADMAP.md
#
# Prerequisites:
#   - PostgreSQL running on localhost:5432, with the benchmark database created
#     (createdb orcher). Override the connection with DATABASE_URL, or the
#     individual ORCHER_BENCH_PGHOST/PGPORT/PGDB/PGUSER variables.
#   - Orchestrator running on localhost:50051 (with benchmark.toml config)
#   - An sdk-rust checkout for the Rust worker (its examples/durable_execution),
#     beside this repository or at SDK_RUST_DIR; the script builds it
#   - TypeScript example built: examples/sdk-ts/durable-execution (optional)
#   - Python example setup: examples/sdk-py/durable-execution (optional)
#

set -e

#==============================================================================
# Configuration
#==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

#------------------------------------------------------------------------------
# Optional: load .env (DATABASE_URL, REDIS_URL, etc.)
#------------------------------------------------------------------------------
# This keeps the script non-interactive while allowing credentials to come from
# your local `.env`. Lines must be KEY=VALUE (no `export` required).
ENV_FILE_CANDIDATES=(
    "${PROJECT_ROOT}/.env"
)
for env_file in "${ENV_FILE_CANDIDATES[@]}"; do
    if [ -f "$env_file" ]; then
        # shellcheck disable=SC1090
        set -a
        . "$env_file"
        set +a
        break
    fi
done

# Helper: parse a postgres:// or postgresql:// URL into PG* vars (bash 3.x)
# Example: postgresql://user:pass@host:5432/dbname
parse_database_url() {
    local url="$1"

    # Trim scheme
    url="${url#postgresql://}"
    url="${url#postgres://}"

    # Split off path (/dbname?query)
    local authority="${url%%/*}"
    local path="${url#*/}"
    local db="${path%%\?*}"

    # Handle optional userinfo@
    local hostport="$authority"
    local userinfo=""
    if echo "$authority" | grep -q "@"; then
        userinfo="${authority%%@*}"
        hostport="${authority#*@}"
    fi

    # user[:pass]
    local user=""
    local pass=""
    if [ -n "$userinfo" ]; then
        user="${userinfo%%:*}"
        if echo "$userinfo" | grep -q ":"; then
            pass="${userinfo#*:}"
        fi
    fi

    # host[:port] (very simple; does not support IPv6 bracket notation)
    local host="${hostport%%:*}"
    local port=""
    if echo "$hostport" | grep -q ":"; then
        port="${hostport#*:}"
    fi

    # Export derived vars only if present (do not clobber explicit settings)
    if [ -n "$host" ] && [ -z "${PGHOST:-}" ]; then export PGHOST="$host"; fi
    if [ -n "$port" ] && [ -z "${PGPORT:-}" ]; then export PGPORT="$port"; fi
    if [ -n "$db" ]   && [ -z "${PGDATABASE:-}" ]; then export PGDATABASE="$db"; fi
    if [ -n "$user" ] && [ -z "${PGUSER:-}" ]; then export PGUSER="$user"; fi
    if [ -n "$pass" ] && [ -z "${PGPASSWORD:-}" ]; then export PGPASSWORD="$pass"; fi
}

# If DATABASE_URL is present (e.g. from `.env`), derive PG* vars for psql
if [ -n "${DATABASE_URL:-}" ]; then
    parse_database_url "$DATABASE_URL"
fi

# The Rust worker is the durable_execution example in an sdk-rust checkout,
# beside this repository by default; the script builds it before running.
SDK_RUST_DIR="${SDK_RUST_DIR:-${PROJECT_ROOT}/../sdk-rust}"
RUST_WORKER_BIN="${SDK_RUST_DIR}/target/release/durable-execution-service"
TS_EXAMPLE_DIR="${PROJECT_ROOT}/examples/sdk-ts/durable-execution"
PY_EXAMPLE_DIR="${PROJECT_ROOT}/examples/sdk-py/durable-execution"
RESULTS_DIR="${PROJECT_ROOT}/test_logs/benchmark_$(date +%Y%m%d_%H%M%S)"

# Default settings
WORKER_COUNTS="1,2,4"
WORKFLOW_COUNT=20
WORKFLOWS_PER_WORKER=50      # Used when AUTO_SCALE_WORKFLOWS=true
WORKFLOWS_TOTAL=""           # If set, use this fixed total instead of WORKFLOW_COUNT
AUTO_SCALE_WORKFLOWS=false   # Scale workflows proportionally with worker count
ITERATIONS=3
TIMEOUT_SECS=120
WARMUP_SECS=3
WORKER_STARTUP_DELAY=3
COOLDOWN_SECS=5
SDK="rust"
SYNC_MATCH_MODE=true
QUICK_MODE=false
VERBOSE=false
SKIP_CONFIG_CHECK=false
CONCURRENT_POLLERS_MODE=false  # Test multi-slot execution context
POLLER_COUNTS="1,4"            # Poller counts to test in concurrent-pollers mode
TS_MODE="direct"               # TypeScript SDK mode (only 'direct' is supported)

# Consistent namespace for all tests
NAMESPACE="benchmark"
# Base task queue - will be suffixed with SDK name to avoid cross-SDK contamination
TASK_QUEUE_BASE="benchmark-queue"
TASK_QUEUE="${TASK_QUEUE_BASE}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

#==============================================================================
# Parse Arguments
#==============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --workers)
            WORKER_COUNTS="${2:-1,2,4}"
            shift 2
            ;;
        --workflows)
            WORKFLOW_COUNT="${2:-20}"
            shift 2
            ;;
        --iterations)
            ITERATIONS="${2:-3}"
            shift 2
            ;;
        --timeout)
            TIMEOUT_SECS="${2:-120}"
            shift 2
            ;;
        --warmup)
            WARMUP_SECS="${2:-3}"
            shift 2
            ;;
        --cooldown)
            COOLDOWN_SECS="${2:-5}"
            shift 2
            ;;
        --sdk)
            SDK="${2:-rust}"
            shift 2
            ;;
        --sync-match)
            SYNC_MATCH_MODE=true
            shift
            ;;
        --quick)
            QUICK_MODE=true
            WORKER_COUNTS="1,2"
            WORKFLOW_COUNT=10
            ITERATIONS=2
            shift
            ;;
        --concurrent-pollers)
            CONCURRENT_POLLERS_MODE=true
            SDK="typescript"
            shift
            ;;
        --poller-counts)
            POLLER_COUNTS="${2:-1,4}"
            shift 2
            ;;
        --ts-mode)
            TS_MODE="${2:-ffi}"
            shift 2
            ;;
        --auto-scale)
            AUTO_SCALE_WORKFLOWS=true
            shift
            ;;
        --workflows-per-worker)
            WORKFLOWS_PER_WORKER="${2:-50}"
            AUTO_SCALE_WORKFLOWS=true
            shift 2
            ;;
        --workflows-total)
            WORKFLOWS_TOTAL="${2:-200}"
            AUTO_SCALE_WORKFLOWS=false
            shift 2
            ;;
        --skip-config-check)
            SKIP_CONFIG_CHECK=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --workers N,M,O     Comma-separated worker counts to test (default: 1,2,4)"
            echo "  --workflows N       Number of concurrent workflows per test (default: 20)"
            echo "  --iterations N      Iterations per configuration (default: 3)"
            echo "  --timeout N         Timeout seconds per workflow (default: 120)"
            echo "  --warmup N          Warmup seconds after starting workers (default: 3)"
            echo "  --cooldown N        Cooldown seconds between tests (default: 5)"
            echo "  --sdk TYPE          SDK to test: rust, typescript, both (default: rust)"
            echo "  --ts-mode MODE      TypeScript mode (deprecated: only 'direct' is supported)"
            echo "  --sync-match        Use Sync-Match - default"
            echo "  --quick             Quick test (1,2 workers, 10 workflows, 2 iterations)"
            echo "  --auto-scale        Scale workflows proportionally with worker count (recommended)"
            echo "  --workflows-per-worker N  Workflows per worker when auto-scaling (default: 50)"
            echo "  --workflows-total N Total workflows (fixed, overrides --workflows)"
            echo "  --concurrent-pollers  Test multi-slot FFI execution (TypeScript SDK poller scaling)"
            echo "  --poller-counts N,M   Poller counts to test in concurrent-pollers mode (default: 1,4)"
            echo "  --skip-config-check Skip orchestrator config verification"
            echo "  --verbose, -v       Show verbose output"
            echo "  --help, -h          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --quick                          # Fast sanity check"
            echo "  $0 --workers 1,2,4,8 --workflows 50 # Full scaling test (50 wf per test)"
            echo "  $0 --workers 1,2,4,8 --auto-scale   # Recommended: scale workflows with workers"
            echo "  $0 --workers 1,2,4,8 --auto-scale --workflows-per-worker 100"
            echo "  $0 --sdk both --workers 1,2         # Compare Rust and TypeScript"
            echo "  $0 --sdk all --workers 1,2          # Compare all SDKs (Rust, TypeScript, Python)"
            echo "  $0 --sdk python --workers 1,2       # Test Python SDK"
            echo "  $0 --sdk typescript --workers 1,2   # Test TypeScript SDK"
            echo "  $0 --concurrent-pollers             # Validate multi-slot execution (1 vs 4 pollers)"
            echo "  $0 --concurrent-pollers --poller-counts 1,2,4,8  # Test multiple poller counts"
            echo ""
            echo "Scaling Guidance:"
            echo "  Each worker defaults to 100 concurrent execution slots."
            echo "  With 8 workers, that's 800 concurrent polls competing for work."
            echo "  To avoid contention, use --auto-scale or ensure:"
            echo "    workflows >= workers * 50 (minimum)"
            echo "    workflows >= workers * 100 (optimal)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

#==============================================================================
# Setup
#==============================================================================

mkdir -p "$RESULTS_DIR"

log() {
    echo -e "${BLUE}[$(date +%H:%M:%S)]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[$(date +%H:%M:%S)] ✓${NC} $1"
}

log_error() {
    echo -e "${RED}[$(date +%H:%M:%S)] ✗${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[$(date +%H:%M:%S)] ⚠${NC} $1"
}

log_header() {
    echo -e "\n${PURPLE}════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${PURPLE}  $1${NC}"
    echo -e "${PURPLE}════════════════════════════════════════════════════════════════════${NC}\n"
}

log_verbose() {
    if [ "$VERBOSE" = true ]; then
        echo -e "${CYAN}[$(date +%H:%M:%S)] [DEBUG]${NC} $1" >&2
    fi
}

#==============================================================================
# TypeScript SDK Build Function
#==============================================================================

build_typescript_sdk() {
    local mode="$1"  # Only "direct" is now supported (FFI mode has been removed)

    log "Building TypeScript SDK..."

    local sdk_dir="${PROJECT_ROOT}/../sdk-ts/packages/sdk"
    local native_dir="${sdk_dir}/native"

    cd "$native_dir"

    # The TypeScript SDK now uses direct bindings by default (no feature flags needed)
    log_verbose "Building native module..."
    cargo build --jobs 2 2>&1 | grep -v "Compiling" | grep -v "Building" || true
    if [ $? -ne 0 ]; then
        log_error "Failed to build native module"
        cd "$PROJECT_ROOT"
        return 1
    fi

    # Copy the built library
    if [ -f "target/debug/liborcher_core.dylib" ]; then
        cp target/debug/liborcher_core.dylib index.node
    elif [ -f "target/debug/liborcher_core.so" ]; then
        cp target/debug/liborcher_core.so index.node
    elif [ -f "target/debug/orcher_core.dll" ]; then
        cp target/debug/orcher_core.dll index.node
    else
        log_error "Could not find built native library"
        cd "$PROJECT_ROOT"
        return 1
    fi

    # Build TypeScript code
    cd "$sdk_dir"
    log_verbose "Building TypeScript SDK..."
    npm run build:ts > /dev/null 2>&1

    # Copy to dist/native for runtime loading
    cp native/index.node dist/native/orcher_core.node

    # Reinstall SDK in example to get the new native module
    log_verbose "Reinstalling SDK in example..."
    local example_dir="${PROJECT_ROOT}/examples/sdk-ts/durable-execution"
    cd "$example_dir"
    rm -rf node_modules/@orcher > /dev/null 2>&1 || true
    npm install "$sdk_dir" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        log_error "Failed to reinstall SDK in example"
        cd "$PROJECT_ROOT"
        return 1
    fi

    cd "$PROJECT_ROOT"
    log_success "TypeScript SDK built"
    return 0
}

# Array to track all worker PIDs
WORKER_PIDS=()

cleanup() {
    log "Cleaning up..."
    for pid in "${WORKER_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    pkill -f "durable-execution-service" 2>/dev/null || true
    pkill -f "node.*dist/main" 2>/dev/null || true
    pkill -f "python.*src.main" 2>/dev/null || true
    sleep 1
}

trap cleanup EXIT

#==============================================================================
# Prerequisite Checks
#==============================================================================

echo -e "${BOLD}"
echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║              ORCHER Unified Performance Benchmark                     ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

log "Checking prerequisites..."

# Check PostgreSQL
#
# Avoid interactive password prompts:
# - Prefer non-interactive psql behavior (--no-password).
# - Allow overriding connection details via env vars:
#   ORCHER_BENCH_PGHOST / ORCHER_BENCH_PGPORT / ORCHER_BENCH_PGDB / ORCHER_BENCH_PGUSER
#   and standard libpq vars like PGPASSWORD, PGHOST, PGPORT, PGDATABASE, PGUSER.
PGHOST_BENCH="${ORCHER_BENCH_PGHOST:-${PGHOST:-localhost}}"
PGPORT_BENCH="${ORCHER_BENCH_PGPORT:-${PGPORT:-5432}}"
PGDB_BENCH="${ORCHER_BENCH_PGDB:-${PGDATABASE:-orcher}}"

# Helper: non-interactive psql probe
psql_probe() {
    local user="$1"
    psql -h "$PGHOST_BENCH" -p "$PGPORT_BENCH" -U "$user" -d "$PGDB_BENCH" --no-password -c "SELECT 1" > /dev/null 2>&1
}

# If ORCHER_BENCH_PGUSER/PGUSER is set, try that first. Otherwise probe the roles
# a local Postgres is likely to have: the container images create `postgres`, our
# own setup docs create `orcher`, and a Homebrew/apt install with peer auth names
# the superuser after the OS account — so try that too before giving up.
CURRENT_OS_USER="$(id -un 2>/dev/null || whoami 2>/dev/null || echo '')"
if [ -n "${ORCHER_BENCH_PGUSER:-${PGUSER:-}}" ]; then
    PGBENCH_USER="${ORCHER_BENCH_PGUSER:-${PGUSER}}"
    if ! psql_probe "$PGBENCH_USER"; then
        log_error "PostgreSQL not available on ${PGHOST_BENCH}:${PGPORT_BENCH} (db=${PGDB_BENCH}, user=${PGBENCH_USER})"
        log_error "If a password is required, set PGPASSWORD (or use a .pgpass file) for non-interactive runs."
        exit 1
    fi
    PG_BENCH_USER="$PGBENCH_USER"
else
    PG_BENCH_USER=""
    for candidate in postgres orcher "$CURRENT_OS_USER"; do
        [ -n "$candidate" ] || continue
        if psql_probe "$candidate"; then
            PG_BENCH_USER="$candidate"
            break
        fi
    done
    if [ -z "$PG_BENCH_USER" ]; then
        log_error "PostgreSQL not available on ${PGHOST_BENCH}:${PGPORT_BENCH} (db=${PGDB_BENCH})"
        log_error "Tried roles: postgres, orcher${CURRENT_OS_USER:+, ${CURRENT_OS_USER}}. Set ORCHER_BENCH_PGUSER to pick one."
        log_error "If a password is required, set PGPASSWORD (or use a .pgpass file) for non-interactive runs."
        log_error "If the database does not exist yet: createdb ${PGDB_BENCH}"
        exit 1
    fi
fi
log_success "PostgreSQL is running (user=${PG_BENCH_USER}, db=${PGDB_BENCH_USER:-${PGDB_BENCH}})"

# Redis is no longer a dependency — the engine runs on PostgreSQL alone.

# Check Orchestrator (port derived from ORCHER_SERVER_URL, default 50051)
ORCHER_GRPC_PORT="$(printf '%s' "${ORCHER_SERVER_URL:-http://localhost:50051}" | sed -E 's#.*:([0-9]+).*#\1#')"
if ! lsof -i ":${ORCHER_GRPC_PORT}" > /dev/null 2>&1; then
    log_error "Orchestrator not running on port ${ORCHER_GRPC_PORT}"
    log_error "Start it with: cargo run --release --bin orchestrator"
    exit 1
fi
log_success "Orchestrator is running"

# Check orchestrator configuration
if [ "$SKIP_CONFIG_CHECK" = false ]; then
    ORCHESTRATOR_CONFIG=$(ps aux | grep orchestrator | grep -v grep | head -1)

    # Recommend benchmark.toml for accurate results
    if echo "$ORCHESTRATOR_CONFIG" | grep -q "benchmark.toml" 2>/dev/null; then
        log_success "Benchmark configuration detected (benchmark.toml)"
    elif echo "$ORCHESTRATOR_CONFIG" | grep -q "sync-match" 2>/dev/null; then
        log_warn "Sync Match config detected, but benchmark.toml is recommended"
        log_warn "The sync-match.toml has aggressive timeouts that cause failures under load"
        log_warn "Restart with: --config services/orchestrator/config/benchmark.toml"
        echo ""
        echo -e "${YELLOW}Continue anyway? (y/N)${NC}"
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            log_error "Aborting. Restart orchestrator with benchmark.toml"
            exit 1
        fi
    else
        log_warn "Could not verify orchestrator configuration"
        log_warn "For accurate benchmarks, use: --config services/orchestrator/config/benchmark.toml"
    fi
fi

# Build the Rust SDK example
if [ "$SDK" = "rust" ] || [ "$SDK" = "both" ] || [ "$SDK" = "all" ]; then
    if [ ! -f "${SDK_RUST_DIR}/examples/durable_execution/Cargo.toml" ]; then
        log_error "sdk-rust checkout not found at ${SDK_RUST_DIR}"
        log_error "Clone https://github.com/orcher-io/sdk-rust beside this repository, or set SDK_RUST_DIR"
        exit 1
    fi
    log "Building the Rust worker in ${SDK_RUST_DIR}..."
    if ! cargo build --release --quiet \
        --manifest-path "${SDK_RUST_DIR}/Cargo.toml" \
        --target-dir "${SDK_RUST_DIR}/target" \
        -p orcher-example-durable-execution; then
        log_error "Failed to build the Rust worker (orcher-example-durable-execution)"
        exit 1
    fi
    log_success "Rust SDK example built"
fi

# Check TypeScript SDK example
if [ "$SDK" = "typescript" ] || [ "$SDK" = "both" ] || [ "$SDK" = "all" ]; then
    if [ ! -f "${TS_EXAMPLE_DIR}/dist/main.js" ]; then
        log_error "TypeScript example not built"
        log_error "Run: cd examples/sdk-ts/durable-execution && npm run build"
        exit 1
    fi
    if [ ! -f "${TS_EXAMPLE_DIR}/dist/load-test.js" ]; then
        log_error "TypeScript load test not built"
        exit 1
    fi

    # Build TypeScript SDK in the requested mode(s)
    if [ "$TS_MODE" = "both" ]; then
        log "Building TypeScript SDK for both FFI and direct-bindings modes..."
        # We'll build each mode when needed during benchmarking
    elif [ "$TS_MODE" = "direct" ]; then
        build_typescript_sdk "direct"
    else
        build_typescript_sdk "ffi"
    fi

    log_success "TypeScript SDK example found"
fi

# Check Python SDK example
if [ "$SDK" = "python" ] || [ "$SDK" = "all" ]; then
    if [ ! -d "${PY_EXAMPLE_DIR}" ]; then
        log_error "Python example directory not found"
        log_error "Expected: ${PY_EXAMPLE_DIR}"
        exit 1
    fi
    # Check for virtual environment or installed orcher package
    if [ -d "${PY_EXAMPLE_DIR}/.venv" ]; then
        PY_VENV="${PY_EXAMPLE_DIR}/.venv"
        PY_PYTHON="${PY_VENV}/bin/python"
        if [ ! -f "$PY_PYTHON" ]; then
            log_error "Python virtual environment found but python binary missing"
            log_error "Run: cd examples/sdk-py/durable-execution && python -m venv .venv && source .venv/bin/activate && pip install -e ."
            exit 1
        fi
        log_success "Python SDK example found (using .venv)"
    else
        # Try system python with orcher installed
        PY_PYTHON="python3"
        if ! $PY_PYTHON -c "import orcher" 2>/dev/null; then
            log_error "Python SDK (orcher) not installed and no .venv found"
            log_error "Run: cd examples/sdk-py/durable-execution && python -m venv .venv && source .venv/bin/activate && pip install -e ../../../../sdk-py"
            exit 1
        fi
        log_success "Python SDK example found (system python)"
    fi
fi

echo ""
log "Configuration:"
if [ "$CONCURRENT_POLLERS_MODE" = true ]; then
    echo "  - Mode: Concurrent Pollers (multi-slot FFI validation)"
    echo "  - Poller counts: ${POLLER_COUNTS}"
else
    echo "  - Mode: $([ "$SYNC_MATCH_MODE" = true ] && echo "Sync-Match" || echo "Redis Streams")"
    echo "  - Worker counts: ${WORKER_COUNTS}"
fi
echo "  - SDK: ${SDK}"
if [ "$AUTO_SCALE_WORKFLOWS" = true ]; then
    echo "  - Workflow scaling: AUTO (${WORKFLOWS_PER_WORKER} per worker)"
elif [ -n "$WORKFLOWS_TOTAL" ]; then
    echo "  - Workflows: ${WORKFLOWS_TOTAL} (fixed total)"
else
    echo "  - Workflows per test: ${WORKFLOW_COUNT}"
fi
echo "  - Iterations: ${ITERATIONS}"
echo "  - Namespace: ${NAMESPACE}"
echo "  - Task Queue: ${TASK_QUEUE}"
echo "  - Results dir: ${RESULTS_DIR}"
echo ""

#==============================================================================
# Helper Functions
#==============================================================================

# Clean Redis, PostgreSQL, and wait for orchestrator to stabilize
reset_environment() {
    log_verbose "Resetting environment..."

    # Kill any existing workers using the cleanup function
    cleanup_stray_workers
    WORKER_PIDS=()

    # Clear ALL Redis orcher keys (comprehensive cleanup across all namespaces/queues)
    # This prevents cross-SDK contamination from previous benchmark runs

    # Clear stale claims in PostgreSQL for ALL test namespaces/queues
    # This prevents cross-SDK contamination where TypeScript workflows get picked up by Python workers
    # Handle all non-terminal statuses: running, retrying, pending, queued
    psql -h "$PGHOST_BENCH" -p "$PGPORT_BENCH" -U "${ORCHER_BENCH_PGUSER:-${PG_BENCH_USER:-orcher}}" -d "$PGDB_BENCH" --no-password -c "
        UPDATE workflow_executions
        SET status = 'cancelled', claimed_by = NULL, claimed_at = NULL, claim_timeout_at = NULL
        WHERE namespace IN ('benchmark', 'default')
        AND status IN ('running', 'retrying', 'pending', 'queued');
    " > /dev/null 2>&1 || true

    # Also clean workflows from all benchmark-related task queues
    psql -h "$PGHOST_BENCH" -p "$PGPORT_BENCH" -U "${ORCHER_BENCH_PGUSER:-${PG_BENCH_USER:-orcher}}" -d "$PGDB_BENCH" --no-password -c "
        UPDATE workflow_executions
        SET status = 'cancelled', claimed_by = NULL, claimed_at = NULL, claim_timeout_at = NULL
        WHERE task_queue IN ('benchmark-queue', 'payment-processing', 'orders')
        AND status IN ('running', 'retrying', 'pending', 'queued');
    " > /dev/null 2>&1 || true

    # Clear pending_requeue flags that could cause re-polling of stale workflows
    psql -h "$PGHOST_BENCH" -p "$PGPORT_BENCH" -U "${ORCHER_BENCH_PGUSER:-${PG_BENCH_USER:-orcher}}" -d "$PGDB_BENCH" --no-password -c "
        UPDATE workflow_executions
        SET pending_requeue = false, pending_requeue_at = NULL
        WHERE pending_requeue = true;
    " > /dev/null 2>&1 || true

    # Clear ALL stuck task executions in test namespaces (no age filter to ensure complete cleanup)
    # Must set started_at and completed_at to satisfy database constraints
    psql -h "$PGHOST_BENCH" -p "$PGPORT_BENCH" -U "${ORCHER_BENCH_PGUSER:-${PG_BENCH_USER:-orcher}}" -d "$PGDB_BENCH" --no-password -c "
        UPDATE task_executions
        SET status = 'failed',
            started_at = COALESCE(started_at, NOW()),
            completed_at = NOW()
        WHERE namespace IN ('benchmark', 'default')
        AND status IN ('running', 'retrying', 'pending', 'scheduled', 'started');
    " > /dev/null 2>&1 || true

    # Also clean task executions from all benchmark-related task queues
    psql -h "$PGHOST_BENCH" -p "$PGPORT_BENCH" -U "${ORCHER_BENCH_PGUSER:-${PG_BENCH_USER:-orcher}}" -d "$PGDB_BENCH" --no-password -c "
        UPDATE task_executions
        SET status = 'failed',
            started_at = COALESCE(started_at, NOW()),
            completed_at = NOW()
        WHERE task_queue IN ('benchmark-queue', 'payment-processing', 'orders')
        AND status IN ('running', 'retrying', 'pending', 'scheduled', 'started');
    " > /dev/null 2>&1 || true

    # Verify cleanup - count any remaining stale workflows
    local stale_count=$(psql -h "$PGHOST_BENCH" -p "$PGPORT_BENCH" -U "${ORCHER_BENCH_PGUSER:-${PG_BENCH_USER:-orcher}}" -d "$PGDB_BENCH" --no-password -tAc "
        SELECT COUNT(*) FROM workflow_executions
        WHERE namespace IN ('benchmark', 'default')
        AND status NOT IN ('completed', 'failed', 'cancelled', 'terminated', 'timed_out');
    " 2>/dev/null || echo "0")

    if [ "$stale_count" -gt 0 ] 2>/dev/null; then
        log_warn "Found $stale_count stale workflows after cleanup, force-failing them..."
        psql -h "$PGHOST_BENCH" -p "$PGPORT_BENCH" -U "${ORCHER_BENCH_PGUSER:-${PG_BENCH_USER:-orcher}}" -d "$PGDB_BENCH" --no-password -c "
            UPDATE workflow_executions
            SET status = 'failed', claimed_by = NULL
            WHERE namespace IN ('benchmark', 'default')
            AND status NOT IN ('completed', 'failed', 'cancelled', 'terminated', 'timed_out');
        " > /dev/null 2>&1 || true
    fi

    # Verify task cleanup - count any remaining stale tasks
    local stale_tasks=$(psql -h "$PGHOST_BENCH" -p "$PGPORT_BENCH" -U "${ORCHER_BENCH_PGUSER:-${PG_BENCH_USER:-orcher}}" -d "$PGDB_BENCH" --no-password -tAc "
        SELECT COUNT(*) FROM task_executions
        WHERE namespace IN ('benchmark', 'default')
        AND status NOT IN ('completed', 'failed', 'cancelled');
    " 2>/dev/null || echo "0")

    if [ "$stale_tasks" -gt 0 ] 2>/dev/null; then
        log_warn "Found $stale_tasks stale tasks after cleanup, force-failing them..."
        psql -h "$PGHOST_BENCH" -p "$PGPORT_BENCH" -U "${ORCHER_BENCH_PGUSER:-${PG_BENCH_USER:-orcher}}" -d "$PGDB_BENCH" --no-password -c "
            UPDATE task_executions
            SET status = 'failed',
                started_at = COALESCE(started_at, NOW()),
                completed_at = NOW()
            WHERE namespace IN ('benchmark', 'default')
            AND status NOT IN ('completed', 'failed', 'cancelled');
        " > /dev/null 2>&1 || true
    fi

    # Wait for orchestrator's in-memory state to expire
    # (WaitersRegistry and HighPerfCache cleanup happens periodically)
    # Increased from 2s to 5s to ensure complete state flush
    sleep 5

    log_verbose "Environment reset complete"
}

# Set SDK-specific task queue to avoid cross-SDK contamination
# Each SDK uses its own queue so stale tasks don't get dispatched to wrong workers
set_sdk_task_queue() {
    local sdk_name=$1
    TASK_QUEUE="${TASK_QUEUE_BASE}-${sdk_name}"
    log_verbose "Using task queue: ${TASK_QUEUE}"
}

# Start Rust workers
start_rust_workers() {
    local worker_count=$1
    local log_prefix=$2

    WORKER_PIDS=()

    for i in $(seq 1 $worker_count); do
        local log_file="${log_prefix}_worker_${i}.log"

        # Use 8 pollers each to match Python SDK configuration for fair comparison
        ORCHER_SERVER_URL="${ORCHER_SERVER_URL:-http://localhost:50051}" \
        ORCHER_NAMESPACE="${NAMESPACE}" \
        ORCHER_TASK_QUEUE="${TASK_QUEUE}" \
        ORCHER_WORKER_ID="rust-worker-${i}" \
        ORCHER_WORKFLOW_POLLER_COUNT="8" \
        ORCHER_TASK_POLLER_COUNT="8" \
        "${RUST_WORKER_BIN}" > "$log_file" 2>&1 &

        local pid=$!
        WORKER_PIDS+=($pid)

        log_verbose "Started Rust worker $i (PID: $pid)"

        # Brief delay between worker starts
        sleep 0.3
    done

    # Wait for workers to initialize
    sleep $WORKER_STARTUP_DELAY

    # Verify all workers started
    local running=0
    for pid in "${WORKER_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            ((running++))
        fi
    done

    echo $running
}

# Start TypeScript worker (single worker mode for now)
start_ts_worker() {
    local log_prefix=$1
    local log_file="${log_prefix}_ts_worker.log"

    WORKER_PIDS=()

    cd "${TS_EXAMPLE_DIR}"
    node dist/main.js > "$log_file" 2>&1 &
    local pid=$!
    WORKER_PIDS+=($pid)
    cd "${PROJECT_ROOT}"

    log_verbose "Started TypeScript worker (PID: $pid)"

    # Wait for worker to initialize
    sleep $WORKER_STARTUP_DELAY

    if kill -0 "$pid" 2>/dev/null; then
        echo 1
    else
        echo 0
    fi
}

# Start TypeScript worker with specific poller count
start_ts_worker_with_pollers() {
    local log_prefix=$1
    local poller_count=${2:-8}  # Default to 8 to match Python SDK
    local log_file="${log_prefix}_ts_worker_${poller_count}pollers.log"

    WORKER_PIDS=()

    cd "${TS_EXAMPLE_DIR}"
    ORCHER_SERVER_URL="${ORCHER_SERVER_URL:-http://localhost:50051}" \
    NAMESPACE="${NAMESPACE}" \
    TASK_QUEUE="${TASK_QUEUE}" \
    WORKFLOW_POLLER_COUNT=$poller_count \
    TASK_POLLER_COUNT=$poller_count \
    node dist/main.js > "$log_file" 2>&1 &
    local pid=$!
    WORKER_PIDS+=($pid)
    cd "${PROJECT_ROOT}"

    log_verbose "Started TypeScript worker with ${poller_count} pollers (PID: $pid)"

    # Wait for worker to initialize
    sleep $WORKER_STARTUP_DELAY

    if kill -0 "$pid" 2>/dev/null; then
        echo 1
    else
        echo 0
    fi
}

# Start Python workers
start_py_workers() {
    local worker_count=$1
    local log_prefix=$2

    WORKER_PIDS=()

    for i in $(seq 1 $worker_count); do
        local log_file="${log_prefix}_worker_${i}.log"

        cd "${PY_EXAMPLE_DIR}"
        # Use default namespace/queue (like TypeScript) to match load_test defaults
        # Use 1 poller each for stability - thread pool implementation needs further tuning
        # for memory efficiency under high concurrency. See runtime.rs for details.
        ORCHER_SERVER_URL="${ORCHER_SERVER_URL:-http://localhost:50051}" \
        NAMESPACE="$NAMESPACE" \
        TASK_QUEUE="$TASK_QUEUE" \
        WORKFLOW_POLLER_COUNT="8" \
        TASK_POLLER_COUNT="8" \
        $PY_PYTHON -m src.main > "$log_file" 2>&1 &

        local pid=$!
        WORKER_PIDS+=($pid)
        cd "${PROJECT_ROOT}"

        log_verbose "Started Python worker $i (PID: $pid)"

        # Brief delay between worker starts
        sleep 0.3
    done

    # Wait for workers to initialize
    sleep $WORKER_STARTUP_DELAY

    # Verify all workers started
    local running=0
    for pid in "${WORKER_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            ((running++))
        fi
    done

    echo $running
}

# Start Python worker with specific poller count
start_py_worker_with_pollers() {
    local log_prefix=$1
    local poller_count=$2
    local log_file="${log_prefix}_py_worker_${poller_count}pollers.log"

    WORKER_PIDS=()

    cd "${PY_EXAMPLE_DIR}"
    # Use default namespace/queue (like TypeScript) to match load_test defaults
    ORCHER_SERVER_URL="${ORCHER_SERVER_URL:-http://localhost:50051}" \
    WORKFLOW_POLLER_COUNT=$poller_count \
    TASK_POLLER_COUNT=$poller_count \
    $PY_PYTHON -m src.main > "$log_file" 2>&1 &

    local pid=$!
    WORKER_PIDS+=($pid)
    cd "${PROJECT_ROOT}"

    log_verbose "Started Python worker with ${poller_count} pollers (PID: $pid)"

    # Wait for worker to initialize
    sleep $WORKER_STARTUP_DELAY

    if kill -0 "$pid" 2>/dev/null; then
        echo 1
    else
        echo 0
    fi
}

# Stop all workers
stop_workers() {
    if [ ${#WORKER_PIDS[@]} -eq 0 ]; then
        return
    fi

    log_verbose "Stopping ${#WORKER_PIDS[@]} worker(s) gracefully..."

    # Send SIGTERM for graceful shutdown
    for pid in "${WORKER_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done

    # Wait for graceful shutdown (increased from 1s to 10s)
    # Python SDK shutdown() has a 5s default timeout, so we wait longer
    local max_wait=10
    local waited=0
    local all_stopped=false

    while [ $waited -lt $max_wait ]; do
        all_stopped=true
        for pid in "${WORKER_PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                all_stopped=false
                break
            fi
        done

        if [ "$all_stopped" = true ]; then
            log_verbose "All workers stopped gracefully after ${waited}s"
            break
        fi

        sleep 1
        ((waited++))
    done

    # Force kill any remaining
    if [ "$all_stopped" = false ]; then
        log_verbose "Force killing remaining workers after ${max_wait}s..."
        for pid in "${WORKER_PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
                log_verbose "Force killed worker PID $pid"
            fi
        done
    fi

    WORKER_PIDS=()
}

# Clean up any stray worker processes from previous runs
cleanup_stray_workers() {
    log_verbose "Checking for stray worker processes..."

    # Kill Python workers
    local py_pids=$(pgrep -f "python.*src\.main" 2>/dev/null || true)
    if [ -n "$py_pids" ]; then
        log_verbose "Found stray Python workers, cleaning up..."
        pkill -TERM -f "python.*src\.main" 2>/dev/null || true
        sleep 2
        pkill -KILL -f "python.*src\.main" 2>/dev/null || true
    fi

    # Kill TypeScript workers
    local ts_pids=$(pgrep -f "node.*dist/service" 2>/dev/null || true)
    if [ -n "$ts_pids" ]; then
        log_verbose "Found stray TypeScript workers, cleaning up..."
        pkill -TERM -f "node.*dist/service" 2>/dev/null || true
        sleep 2
        pkill -KILL -f "node.*dist/service" 2>/dev/null || true
    fi

    # Kill Rust workers
    local rust_pids=$(pgrep -f "durable-execution-service" 2>/dev/null || true)
    if [ -n "$rust_pids" ]; then
        log_verbose "Found stray Rust workers, cleaning up..."
        pkill -TERM -f "durable-execution-service" 2>/dev/null || true
        sleep 2
        pkill -KILL -f "durable-execution-service" 2>/dev/null || true
    fi

    log_verbose "Stray worker cleanup complete"
}

# Run concurrent Rust workflows
run_rust_workflows() {
    local workflow_count=$1
    local log_prefix=$2

    local start_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')
    local client_pids=()
    local result_files=()

    # Launch all workflows concurrently
    for i in $(seq 1 $workflow_count); do
        local result_file="${log_prefix}_client_${i}.log"
        result_files+=("$result_file")

        (
            ORCHER_SERVER_URL="${ORCHER_SERVER_URL:-http://localhost:50051}" \
            ORCHER_NAMESPACE="${NAMESPACE}" \
            ORCHER_TASK_QUEUE="${TASK_QUEUE}" \
            "${RUST_WORKER_BIN}" --client --skip-idempotency-check > "$result_file" 2>&1 &
            local inner_pid=$!

            # Wait with timeout
            local elapsed=0
            while kill -0 $inner_pid 2>/dev/null && [ $elapsed -lt $TIMEOUT_SECS ]; do
                sleep 1
                ((elapsed++))
            done

            if kill -0 $inner_pid 2>/dev/null; then
                kill -9 $inner_pid 2>/dev/null || true
                wait $inner_pid 2>/dev/null || true
                echo 124 > "${result_file}.exit"
            else
                wait $inner_pid
                echo $? > "${result_file}.exit"
            fi
        ) &
        client_pids+=($!)
    done

    # Wait for all clients to complete
    for pid in "${client_pids[@]}"; do
        wait $pid 2>/dev/null || true
    done

    local end_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')
    local duration_ms=$((end_ms - start_ms))

    # Count successes
    local successes=0
    local failures=0
    for result_file in "${result_files[@]}"; do
        if grep -q "Workflow completed successfully" "$result_file" 2>/dev/null; then
            ((successes++))
        else
            ((failures++))
        fi
    done

    echo "${duration_ms}:${successes}:${failures}"
}

# Run TypeScript load test
run_ts_workflows() {
    local workflow_count=$1
    local log_prefix=$2
    local result_file="${log_prefix}_ts_loadtest.log"

    local start_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')

    cd "${TS_EXAMPLE_DIR}"
    NUM_WORKFLOWS=$workflow_count \
    ORCHER_SERVER_URL="${ORCHER_SERVER_URL:-http://localhost:50051}" \
    NAMESPACE="${NAMESPACE}" \
    TASK_QUEUE="${TASK_QUEUE}" \
    node dist/load-test.js > "$result_file" 2>&1
    local exit_code=$?
    cd "${PROJECT_ROOT}"

    local end_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')
    local duration_ms=$((end_ms - start_ms))

    # Parse results from log
    # Use BSD grep compatible syntax (macOS doesn't support -P)
    local completed=$(grep -E 'Completed:' "$result_file" 2>/dev/null | grep -oE '[0-9]+' | head -1)
    local failed=$(grep -E 'Failed:' "$result_file" 2>/dev/null | grep -oE '[0-9]+' | head -1)

    # Default to 0 if not found
    completed=${completed:-0}
    failed=${failed:-$workflow_count}

    if [ "$completed" = "0" ] && [ $exit_code -eq 0 ]; then
        # Try alternative parsing
        completed=$workflow_count
        failed=0
    fi

    echo "${duration_ms}:${completed}:${failed}"
}

# Run Python load test
run_py_workflows() {
    local workflow_count=$1
    local log_prefix=$2
    local result_file="${log_prefix}_py_loadtest.log"

    local start_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')

    cd "${PY_EXAMPLE_DIR}"
    # Use default namespace/queue (like TypeScript) - don't override
    NUM_WORKFLOWS=$workflow_count \
    ORCHER_SERVER_URL="${ORCHER_SERVER_URL:-http://localhost:50051}" \
    NAMESPACE="$NAMESPACE" \
    TASK_QUEUE="$TASK_QUEUE" \
    $PY_PYTHON -m src.load_test > "$result_file" 2>&1
    local exit_code=$?
    cd "${PROJECT_ROOT}"

    local end_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')
    local duration_ms=$((end_ms - start_ms))

    # Parse results from log (Python load_test.py output format)
    # Format: "   Completed:           X"
    # Use BSD grep compatible syntax (macOS doesn't support -P)
    local completed=$(grep -E 'Completed:' "$result_file" 2>/dev/null | grep -oE '[0-9]+' | head -1)
    local failed=$(grep -E 'Failed:' "$result_file" 2>/dev/null | grep -oE '[0-9]+' | head -1)

    # Default to 0 if not found
    completed=${completed:-0}
    failed=${failed:-$workflow_count}

    if [ "$completed" = "0" ] && [ $exit_code -eq 0 ]; then
        # Try alternative parsing - check for success message
        if grep -q "All workflows completed successfully" "$result_file" 2>/dev/null; then
            completed=$workflow_count
            failed=0
        fi
    fi

    echo "${duration_ms}:${completed}:${failed}"
}

# Calculate statistics from array of values
calc_stats() {
    local values=("$@")
    local count=${#values[@]}

    if [ $count -eq 0 ]; then
        echo "0:0:0"
        return
    fi

    # Sort values (bash 3.x compatible)
    local sorted_str=$(printf '%s\n' "${values[@]}" | sort -n)
    local sorted=()
    while IFS= read -r line; do
        sorted+=("$line")
    done <<< "$sorted_str"

    # Calculate min, max, avg
    local min=${sorted[0]}
    local max=${sorted[$((count-1))]}
    local sum=0
    for val in "${values[@]}"; do
        sum=$((sum + val))
    done
    local avg=$((sum / count))

    echo "${min}:${max}:${avg}"
}

#==============================================================================
# Benchmark Functions
#==============================================================================

# Calculate workflow count for a given worker count
calculate_workflow_count() {
    local worker_count=$1

    if [ -n "$WORKFLOWS_TOTAL" ]; then
        # Fixed total workflows
        echo "$WORKFLOWS_TOTAL"
    elif [ "$AUTO_SCALE_WORKFLOWS" = true ]; then
        # Scale with workers: workflows = workers * workflows_per_worker
        echo $((worker_count * WORKFLOWS_PER_WORKER))
    else
        # Use fixed workflow count
        echo "$WORKFLOW_COUNT"
    fi
}

run_rust_benchmark() {
    local worker_count=$1
    local log_prefix="${RESULTS_DIR}/rust_${worker_count}w"

    # Set SDK-specific task queue to avoid cross-SDK contamination
    set_sdk_task_queue "rust"

    # Calculate workflow count for this worker configuration
    local effective_workflow_count=$(calculate_workflow_count $worker_count)

    log_header "Rust SDK: ${worker_count} Worker(s), ${effective_workflow_count} Workflows" >&2

    # Reset environment
    reset_environment

    # Start workers
    log "Starting ${worker_count} Rust worker(s)..." >&2
    local running=$(start_rust_workers $worker_count "$log_prefix")

    if [ "$running" -ne "$worker_count" ]; then
        log_error "Only ${running}/${worker_count} workers started" >&2
        stop_workers
        echo "0:0:0:0:0:0:0"
        return
    fi
    log_success "All ${worker_count} worker(s) ready" >&2

    # Warmup
    log "Warming up for ${WARMUP_SECS}s..." >&2
    sleep $WARMUP_SECS

    # Run iterations
    local durations=()
    local total_successes=0
    local total_failures=0

    for iter in $(seq 1 $ITERATIONS); do
        log "Running iteration ${iter}/${ITERATIONS} (${effective_workflow_count} workflows)..." >&2

        local result=$(run_rust_workflows $effective_workflow_count "${log_prefix}_iter${iter}")
        local duration=$(echo "$result" | cut -d: -f1)
        local successes=$(echo "$result" | cut -d: -f2)
        local failures=$(echo "$result" | cut -d: -f3)

        durations+=($duration)
        total_successes=$((total_successes + successes))
        total_failures=$((total_failures + failures))

        local throughput=$(python3 -c "print(f'{${effective_workflow_count} / (${duration} / 1000.0):.2f}')" 2>/dev/null || echo "N/A")

        if [ "$successes" -eq "$effective_workflow_count" ]; then
            log_success "Iteration ${iter}: ${duration}ms, ${successes}/${effective_workflow_count} success, ${throughput} wf/s" >&2
        else
            log_warn "Iteration ${iter}: ${duration}ms, ${successes}/${effective_workflow_count} success, ${failures} failed" >&2
        fi

        # Brief pause between iterations
        sleep 2
    done

    # Stop workers
    log "Stopping workers..." >&2
    stop_workers

    # Calculate stats
    local stats=$(calc_stats "${durations[@]}")
    local min_ms=$(echo "$stats" | cut -d: -f1)
    local max_ms=$(echo "$stats" | cut -d: -f2)
    local avg_ms=$(echo "$stats" | cut -d: -f3)

    local total_workflows=$((ITERATIONS * effective_workflow_count))
    local success_rate=$(python3 -c "print(f'{${total_successes} / ${total_workflows} * 100:.1f}')" 2>/dev/null || echo "0")
    local throughput=$(python3 -c "print(f'{${effective_workflow_count} / (${avg_ms} / 1000.0):.2f}')" 2>/dev/null || echo "0")

    # Print results to stderr (display)
    echo "" >&2
    echo -e "${CYAN}┌─────────────────────────────────────────────────────────────────┐${NC}" >&2
    echo -e "${CYAN}│  Rust SDK - ${worker_count} Worker(s) Results                              │${NC}" >&2
    echo -e "${CYAN}├─────────────────────────────────────────────────────────────────┤${NC}" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Avg Duration:" "${avg_ms} ms" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Min/Max:" "${min_ms} / ${max_ms} ms" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Throughput:" "${throughput} wf/s" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Success Rate:" "${success_rate}%" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Successes:" "${total_successes}/${total_workflows}" >&2
    echo -e "${CYAN}└─────────────────────────────────────────────────────────────────┘${NC}" >&2
    echo "" >&2

    # Return results to stdout (for capture)
    # Format: avg_ms:min_ms:max_ms:throughput:success_rate:successes:total:workflow_count
    echo "${avg_ms}:${min_ms}:${max_ms}:${throughput}:${success_rate}:${total_successes}:${total_workflows}:${effective_workflow_count}"
}

run_ts_benchmark() {
    local log_prefix="${RESULTS_DIR}/ts_1w"
    local default_poller_count=8  # Match Python SDK poller count for fair comparison

    # Set SDK-specific task queue to avoid cross-SDK contamination
    set_sdk_task_queue "typescript"

    log_header "TypeScript SDK: 1 Worker, ${WORKFLOW_COUNT} Workflows" >&2

    # Reset environment
    reset_environment

    # Start worker with default poller count
    log "Starting TypeScript worker with ${default_poller_count} poller(s)..." >&2
    local running=$(start_ts_worker_with_pollers "$log_prefix" $default_poller_count)

    if [ "$running" -ne 1 ]; then
        log_error "TypeScript worker failed to start" >&2
        stop_workers
        echo "0:0:0:0:0:0:0"
        return
    fi
    log_success "TypeScript worker ready" >&2

    # Warmup
    log "Warming up for ${WARMUP_SECS}s..." >&2
    sleep $WARMUP_SECS

    # Run iterations
    local durations=()
    local total_successes=0
    local total_failures=0

    for iter in $(seq 1 $ITERATIONS); do
        log "Running iteration ${iter}/${ITERATIONS}..." >&2

        local result=$(run_ts_workflows $WORKFLOW_COUNT "${log_prefix}_iter${iter}")
        local duration=$(echo "$result" | cut -d: -f1)
        local successes=$(echo "$result" | cut -d: -f2)
        local failures=$(echo "$result" | cut -d: -f3)

        durations+=($duration)
        total_successes=$((total_successes + successes))
        total_failures=$((total_failures + failures))

        local throughput=$(python3 -c "print(f'{${WORKFLOW_COUNT} / (${duration} / 1000.0):.2f}')" 2>/dev/null || echo "N/A")

        if [ "$successes" -eq "$WORKFLOW_COUNT" ]; then
            log_success "Iteration ${iter}: ${duration}ms, ${successes}/${WORKFLOW_COUNT} success, ${throughput} wf/s" >&2
        else
            log_warn "Iteration ${iter}: ${duration}ms, ${successes}/${WORKFLOW_COUNT} success, ${failures} failed" >&2
        fi

        # Brief pause between iterations
        sleep 2
    done

    # Stop workers
    log "Stopping workers..." >&2
    stop_workers

    # Calculate stats
    local stats=$(calc_stats "${durations[@]}")
    local min_ms=$(echo "$stats" | cut -d: -f1)
    local max_ms=$(echo "$stats" | cut -d: -f2)
    local avg_ms=$(echo "$stats" | cut -d: -f3)

    local total_workflows=$((ITERATIONS * WORKFLOW_COUNT))
    local success_rate=$(python3 -c "print(f'{${total_successes} / ${total_workflows} * 100:.1f}')" 2>/dev/null || echo "0")
    local throughput=$(python3 -c "print(f'{${WORKFLOW_COUNT} / (${avg_ms} / 1000.0):.2f}')" 2>/dev/null || echo "0")

    # Print results to stderr (display)
    echo "" >&2
    echo -e "${CYAN}┌─────────────────────────────────────────────────────────────────┐${NC}" >&2
    echo -e "${CYAN}│  TypeScript SDK - 1 Worker Results                              │${NC}" >&2
    echo -e "${CYAN}├─────────────────────────────────────────────────────────────────┤${NC}" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Avg Duration:" "${avg_ms} ms" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Min/Max:" "${min_ms} / ${max_ms} ms" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Throughput:" "${throughput} wf/s" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Success Rate:" "${success_rate}%" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Successes:" "${total_successes}/${total_workflows}" >&2
    echo -e "${CYAN}└─────────────────────────────────────────────────────────────────┘${NC}" >&2
    echo "" >&2

    # Return results to stdout (for capture)
    echo "${avg_ms}:${min_ms}:${max_ms}:${throughput}:${success_rate}:${total_successes}:${total_workflows}"
}

# Python SDK benchmark with worker scaling
run_python_benchmark() {
    local worker_count=$1
    local log_prefix="${RESULTS_DIR}/py_${worker_count}w"

    # Set SDK-specific task queue to avoid cross-SDK contamination
    set_sdk_task_queue "python"

    # Calculate workflow count for this worker configuration
    local effective_workflow_count=$(calculate_workflow_count $worker_count)

    log_header "Python SDK: ${worker_count} Worker(s), ${effective_workflow_count} Workflows" >&2

    # Reset environment
    reset_environment

    # Start workers
    log "Starting ${worker_count} Python worker(s)..." >&2
    local running=$(start_py_workers $worker_count "$log_prefix")

    if [ "$running" -ne "$worker_count" ]; then
        log_error "Only ${running}/${worker_count} workers started" >&2
        stop_workers
        echo "0:0:0:0:0:0:0:${effective_workflow_count}"
        return
    fi
    log_success "All ${worker_count} worker(s) ready" >&2

    # Warmup
    log "Warming up for ${WARMUP_SECS}s..." >&2
    sleep $WARMUP_SECS

    # Run iterations
    local durations=()
    local total_successes=0
    local total_failures=0

    for iter in $(seq 1 $ITERATIONS); do
        log "Running iteration ${iter}/${ITERATIONS} (${effective_workflow_count} workflows)..." >&2

        local result=$(run_py_workflows $effective_workflow_count "${log_prefix}_iter${iter}")
        local duration=$(echo "$result" | cut -d: -f1)
        local successes=$(echo "$result" | cut -d: -f2)
        local failures=$(echo "$result" | cut -d: -f3)

        durations+=($duration)
        total_successes=$((total_successes + successes))
        total_failures=$((total_failures + failures))

        local throughput=$(python3 -c "print(f'{${effective_workflow_count} / (${duration} / 1000.0):.2f}')" 2>/dev/null || echo "N/A")

        if [ "$successes" -eq "$effective_workflow_count" ]; then
            log_success "Iteration ${iter}: ${duration}ms, ${successes}/${effective_workflow_count} success, ${throughput} wf/s" >&2
        else
            log_warn "Iteration ${iter}: ${duration}ms, ${successes}/${effective_workflow_count} success, ${failures} failed" >&2
        fi

        # Brief pause between iterations
        sleep 2
    done

    # Stop workers
    log "Stopping workers..." >&2
    stop_workers

    # Calculate stats
    local stats=$(calc_stats "${durations[@]}")
    local min_ms=$(echo "$stats" | cut -d: -f1)
    local max_ms=$(echo "$stats" | cut -d: -f2)
    local avg_ms=$(echo "$stats" | cut -d: -f3)

    local total_workflows=$((ITERATIONS * effective_workflow_count))
    local success_rate=$(python3 -c "print(f'{${total_successes} / ${total_workflows} * 100:.1f}')" 2>/dev/null || echo "0")
    local throughput=$(python3 -c "print(f'{${effective_workflow_count} / (${avg_ms} / 1000.0):.2f}')" 2>/dev/null || echo "0")

    # Print results to stderr (display)
    echo "" >&2
    echo -e "${CYAN}┌─────────────────────────────────────────────────────────────────┐${NC}" >&2
    echo -e "${CYAN}│  Python SDK - ${worker_count} Worker(s) Results                              │${NC}" >&2
    echo -e "${CYAN}├─────────────────────────────────────────────────────────────────┤${NC}" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Avg Duration:" "${avg_ms} ms" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Min/Max:" "${min_ms} / ${max_ms} ms" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Throughput:" "${throughput} wf/s" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Success Rate:" "${success_rate}%" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Successes:" "${total_successes}/${total_workflows}" >&2
    echo -e "${CYAN}└─────────────────────────────────────────────────────────────────┘${NC}" >&2
    echo "" >&2

    # Return results to stdout (for capture)
    # Format: avg_ms:min_ms:max_ms:throughput:success_rate:successes:total:workflow_count
    echo "${avg_ms}:${min_ms}:${max_ms}:${throughput}:${success_rate}:${total_successes}:${total_workflows}:${effective_workflow_count}"
}

# Run TypeScript benchmark with specific poller count (for concurrent-pollers mode)
run_ts_concurrent_pollers_benchmark() {
    local poller_count=$1
    local log_prefix="${RESULTS_DIR}/ts_${poller_count}pollers"

    log_header "TypeScript SDK: ${poller_count} Poller(s), ${WORKFLOW_COUNT} Workflows" >&2

    # Reset environment
    reset_environment

    # Start worker with specified poller count
    log "Starting TypeScript worker with ${poller_count} poller(s)..." >&2
    local running=$(start_ts_worker_with_pollers "$log_prefix" $poller_count)

    if [ "$running" -ne 1 ]; then
        log_error "TypeScript worker failed to start" >&2
        stop_workers
        echo "0:0:0:0:0:0:0"
        return
    fi
    log_success "TypeScript worker ready" >&2

    # Warmup
    log "Warming up for ${WARMUP_SECS}s..." >&2
    sleep $WARMUP_SECS

    # Run iterations
    local durations=()
    local total_successes=0
    local total_failures=0

    for iter in $(seq 1 $ITERATIONS); do
        log "Running iteration ${iter}/${ITERATIONS}..." >&2

        local result=$(run_ts_workflows $WORKFLOW_COUNT "${log_prefix}_iter${iter}")
        local duration=$(echo "$result" | cut -d: -f1)
        local successes=$(echo "$result" | cut -d: -f2)
        local failures=$(echo "$result" | cut -d: -f3)

        durations+=($duration)
        total_successes=$((total_successes + successes))
        total_failures=$((total_failures + failures))

        local throughput=$(python3 -c "print(f'{${WORKFLOW_COUNT} / (${duration} / 1000.0):.2f}')" 2>/dev/null || echo "N/A")

        if [ "$successes" -eq "$WORKFLOW_COUNT" ]; then
            log_success "Iteration ${iter}: ${duration}ms, ${successes}/${WORKFLOW_COUNT} success, ${throughput} wf/s" >&2
        else
            log_warn "Iteration ${iter}: ${duration}ms, ${successes}/${WORKFLOW_COUNT} success, ${failures} failed" >&2
        fi

        # Brief pause between iterations
        sleep 2
    done

    # Stop workers
    log "Stopping workers..." >&2
    stop_workers

    # Calculate stats
    local stats=$(calc_stats "${durations[@]}")
    local min_ms=$(echo "$stats" | cut -d: -f1)
    local max_ms=$(echo "$stats" | cut -d: -f2)
    local avg_ms=$(echo "$stats" | cut -d: -f3)

    local total_workflows=$((ITERATIONS * WORKFLOW_COUNT))
    local success_rate=$(python3 -c "print(f'{${total_successes} / ${total_workflows} * 100:.1f}')" 2>/dev/null || echo "0")
    local throughput=$(python3 -c "print(f'{${WORKFLOW_COUNT} / (${avg_ms} / 1000.0):.2f}')" 2>/dev/null || echo "0")

    # Print results to stderr (display)
    echo "" >&2
    echo -e "${CYAN}┌─────────────────────────────────────────────────────────────────┐${NC}" >&2
    echo -e "${CYAN}│  TypeScript SDK - 1 Worker Results                              │${NC}" >&2
    echo -e "${CYAN}├─────────────────────────────────────────────────────────────────┤${NC}" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Avg Duration:" "${avg_ms} ms" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Min/Max:" "${min_ms} / ${max_ms} ms" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Throughput:" "${throughput} wf/s" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Success Rate:" "${success_rate}%" >&2
    printf "${CYAN}│${NC}  %-20s %40s ${CYAN}│${NC}\n" "Successes:" "${total_successes}/${total_workflows}" >&2
    echo -e "${CYAN}└─────────────────────────────────────────────────────────────────┘${NC}" >&2
    echo "" >&2

    # Return results to stdout (for capture)
    echo "${avg_ms}:${min_ms}:${max_ms}:${throughput}:${success_rate}:${total_successes}:${total_workflows}"
}

#==============================================================================
# Concurrent Pollers Benchmark (validates multi-slot FFI)
#==============================================================================

run_concurrent_pollers_benchmark() {
    log_header "Concurrent Pollers Benchmark (Multi-Slot FFI Validation)"

    echo -e "${BOLD}╔════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║     TypeScript SDK - Concurrent Poller Validation                  ║${NC}"
    echo -e "${BOLD}║                                                                    ║${NC}"
    echo -e "${BOLD}║     Validates multi-slot FFI execution context (DashMap)           ║${NC}"
    echo -e "${BOLD}╚════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    # Parse poller counts
    IFS=',' read -ra POLLER_COUNT_ARRAY <<< "$POLLER_COUNTS"

    # Store results for comparison
    local results=()
    local baseline_throughput=""

    for poller_count in "${POLLER_COUNT_ARRAY[@]}"; do
        result=$(run_ts_concurrent_pollers_benchmark $poller_count)

        # Parse result
        avg_ms=$(echo "$result" | cut -d: -f1)
        throughput=$(echo "$result" | cut -d: -f4)
        success_rate=$(echo "$result" | cut -d: -f5)

        results+=("${poller_count}:${throughput}:${success_rate}")

        # Save to CSV
        echo "typescript,1,${WORKFLOW_COUNT},${ITERATIONS},${avg_ms},0,0,${throughput},${success_rate},0,0,pollers_${poller_count}" >> "${RESULTS_DIR}/results.csv"

        # Store baseline
        if [ -z "$baseline_throughput" ]; then
            baseline_throughput="$throughput"
        fi

        # Cooldown between tests (bash 3.x compatible - no negative array indices)
        local last_index=$((${#POLLER_COUNT_ARRAY[@]} - 1))
        local last_poller="${POLLER_COUNT_ARRAY[$last_index]}"
        if [ "$poller_count" != "$last_poller" ]; then
            log "Cooling down for ${COOLDOWN_SECS}s before next test..."
            sleep $COOLDOWN_SECS
        fi
    done

    # Generate comparison summary
    echo ""
    echo -e "${BOLD}════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}  Concurrent Pollers Performance Comparison${NC}"
    echo -e "${BOLD}════════════════════════════════════════════════════════════════════${NC}"
    echo ""

    echo -e "${BOLD}┌──────────────┬──────────────┬──────────────┬────────────────┐${NC}"
    echo -e "${BOLD}│ Pollers      │ Throughput   │ Success Rate │ Speedup        │${NC}"
    echo -e "${BOLD}├──────────────┼──────────────┼──────────────┼────────────────┤${NC}"

    for result in "${results[@]}"; do
        local pollers=$(echo "$result" | cut -d: -f1)
        local throughput=$(echo "$result" | cut -d: -f2)
        local success_rate=$(echo "$result" | cut -d: -f3)

        local first_poller="${POLLER_COUNT_ARRAY[0]}"
        if [ "$pollers" = "$first_poller" ]; then
            local speedup="1.00x (baseline)"
        else
            local speedup=$(python3 -c "print(f'{float(${throughput}) / float(${baseline_throughput}):.2f}x')" 2>/dev/null || echo "N/A")
        fi

        printf "${BOLD}│${NC} %-12s ${BOLD}│${NC} %12s ${BOLD}│${NC} %12s ${BOLD}│${NC} %14s ${BOLD}│${NC}\n" \
            "${pollers} poller(s)" "${throughput} wf/s" "${success_rate}%" "$speedup"
    done

    echo -e "${BOLD}└──────────────┴──────────────┴──────────────┴────────────────┘${NC}"
    echo ""

    # Analyze results (bash 3.x compatible)
    local results_count=${#results[@]}
    if [ $results_count -ge 2 ]; then
        local first_throughput=$(echo "${results[0]}" | cut -d: -f2)
        local last_index=$((results_count - 1))
        local last_throughput=$(echo "${results[$last_index]}" | cut -d: -f2)
        local final_speedup=$(python3 -c "print(round(float(${last_throughput}) / float(${first_throughput}), 2))" 2>/dev/null || echo "0")

        echo -e "${CYAN}Analysis:${NC}"
        if (( $(echo "$final_speedup > 1.5" | bc -l 2>/dev/null || echo 0) )); then
            echo -e "  ${GREEN}✓ Multi-slot FFI execution is working correctly!${NC}"
            echo -e "  ${GREEN}  Speedup of ${final_speedup}x demonstrates concurrent polling.${NC}"
        elif (( $(echo "$final_speedup > 1.0" | bc -l 2>/dev/null || echo 0) )); then
            echo -e "  ${YELLOW}⚠ Some speedup observed (${final_speedup}x), but less than expected.${NC}"
            echo "    This may be due to other bottlenecks (network, DB, orchestrator)."
        else
            echo -e "  ${RED}✗ No speedup observed - multi-slot may not be working correctly.${NC}"
            echo "    Check worker logs for errors or race conditions."
        fi
        echo ""
    fi
}

#==============================================================================
# Main Benchmark Loop
#==============================================================================

main() {
    # Handle concurrent pollers mode separately
    if [ "$CONCURRENT_POLLERS_MODE" = true ]; then
        # Initialize results CSV
        echo "sdk,workers,workflows,iterations,avg_ms,min_ms,max_ms,throughput_wf_s,success_rate,successes,total,scaling_mode" > "${RESULTS_DIR}/results.csv"

        run_concurrent_pollers_benchmark

        log_success "Results saved to ${RESULTS_DIR}/results.csv"
        exit 0
    fi

    # Initialize results CSV
    echo "sdk,workers,workflows,iterations,avg_ms,min_ms,max_ms,throughput_wf_s,success_rate,successes,total,scaling_mode" > "${RESULTS_DIR}/results.csv"

    # Determine scaling mode for logging
    local scaling_mode="fixed"
    if [ "$AUTO_SCALE_WORKFLOWS" = true ]; then
        scaling_mode="auto_${WORKFLOWS_PER_WORKER}_per_worker"
    elif [ -n "$WORKFLOWS_TOTAL" ]; then
        scaling_mode="fixed_total_${WORKFLOWS_TOTAL}"
    fi

    # Parse worker counts
    IFS=',' read -ra WORKER_COUNT_ARRAY <<< "$WORKER_COUNTS"

    # Store results for summary
    RUST_RESULTS=()
    TS_RESULTS=()
    TS_DIRECT_RESULTS=()

    # Run Rust benchmarks
    if [ "$SDK" = "rust" ] || [ "$SDK" = "both" ] || [ "$SDK" = "all" ]; then
        for worker_count in "${WORKER_COUNT_ARRAY[@]}"; do
            result=$(run_rust_benchmark $worker_count)

            # Parse result (now includes workflow_count as 8th field)
            avg_ms=$(echo "$result" | cut -d: -f1)
            min_ms=$(echo "$result" | cut -d: -f2)
            max_ms=$(echo "$result" | cut -d: -f3)
            throughput=$(echo "$result" | cut -d: -f4)
            success_rate=$(echo "$result" | cut -d: -f5)
            successes=$(echo "$result" | cut -d: -f6)
            total=$(echo "$result" | cut -d: -f7)
            effective_workflows=$(echo "$result" | cut -d: -f8)

            # Save to CSV
            echo "rust,${worker_count},${effective_workflows},${ITERATIONS},${avg_ms},${min_ms},${max_ms},${throughput},${success_rate},${successes},${total},${scaling_mode}" >> "${RESULTS_DIR}/results.csv"

            # Store for summary
            RUST_RESULTS+=("${worker_count}:${throughput}:${success_rate}")

            # Cooldown between tests
            local last_index=$((${#WORKER_COUNT_ARRAY[@]} - 1))
            local last_worker="${WORKER_COUNT_ARRAY[$last_index]}"
            if [ "$worker_count" != "$last_worker" ]; then
                log "Cooling down for ${COOLDOWN_SECS}s before next test..."
                sleep $COOLDOWN_SECS
            fi
        done
    fi

    # Run TypeScript benchmarks
    if [ "$SDK" = "typescript" ] || [ "$SDK" = "both" ] || [ "$SDK" = "all" ]; then
        # Determine which modes to test
        local ts_modes_to_test=()
        if [ "$TS_MODE" = "both" ]; then
            ts_modes_to_test=("ffi" "direct")
        else
            ts_modes_to_test=("$TS_MODE")
        fi

        # Run benchmark for each mode
        for ts_mode in "${ts_modes_to_test[@]}"; do
            # Build SDK in the appropriate mode
            build_typescript_sdk "$ts_mode"

            # Run benchmark
            log_header "TypeScript SDK (${ts_mode} mode): 1 Worker, ${WORKFLOW_COUNT} Workflows"
            result=$(run_ts_benchmark)

            # Parse result
            avg_ms=$(echo "$result" | cut -d: -f1)
            min_ms=$(echo "$result" | cut -d: -f2)
            max_ms=$(echo "$result" | cut -d: -f3)
            throughput=$(echo "$result" | cut -d: -f4)
            success_rate=$(echo "$result" | cut -d: -f5)
            successes=$(echo "$result" | cut -d: -f6)
            total=$(echo "$result" | cut -d: -f7)

            # Save to CSV with mode suffix
            echo "typescript-${ts_mode},1,${WORKFLOW_COUNT},${ITERATIONS},${avg_ms},${min_ms},${max_ms},${throughput},${success_rate},${successes},${total},${scaling_mode}" >> "${RESULTS_DIR}/results.csv"

            # Store for summary
            if [ "$ts_mode" = "ffi" ]; then
                TS_RESULTS+=("1:${throughput}:${success_rate}")
            else
                TS_DIRECT_RESULTS+=("1:${throughput}:${success_rate}")
            fi

            # Cooldown between modes
            if [ "$TS_MODE" = "both" ] && [ "$ts_mode" = "ffi" ]; then
                log "Cooling down for ${COOLDOWN_SECS}s before testing direct-bindings mode..."
                sleep $COOLDOWN_SECS
            fi
        done
    fi

    # Run Python benchmarks
    if [ "$SDK" = "python" ] || [ "$SDK" = "all" ]; then
        PY_RESULTS=()
        for worker_count in "${WORKER_COUNT_ARRAY[@]}"; do
            result=$(run_python_benchmark $worker_count)

            # Parse result (now includes workflow_count as 8th field)
            avg_ms=$(echo "$result" | cut -d: -f1)
            min_ms=$(echo "$result" | cut -d: -f2)
            max_ms=$(echo "$result" | cut -d: -f3)
            throughput=$(echo "$result" | cut -d: -f4)
            success_rate=$(echo "$result" | cut -d: -f5)
            successes=$(echo "$result" | cut -d: -f6)
            total=$(echo "$result" | cut -d: -f7)
            effective_workflows=$(echo "$result" | cut -d: -f8)

            # Save to CSV
            echo "python,${worker_count},${effective_workflows},${ITERATIONS},${avg_ms},${min_ms},${max_ms},${throughput},${success_rate},${successes},${total},${scaling_mode}" >> "${RESULTS_DIR}/results.csv"

            # Store for summary
            PY_RESULTS+=("${worker_count}:${throughput}:${success_rate}")

            # Cooldown between tests
            local last_index=$((${#WORKER_COUNT_ARRAY[@]} - 1))
            local last_worker="${WORKER_COUNT_ARRAY[$last_index]}"
            if [ "$worker_count" != "$last_worker" ]; then
                log "Cooling down for ${COOLDOWN_SECS}s before next test..."
                sleep $COOLDOWN_SECS
            fi
        done
    fi

    # Generate summary
    generate_summary
}

#==============================================================================
# Summary Generation
#==============================================================================

generate_summary() {
    log_header "Benchmark Summary"

    echo -e "${BOLD}┌─────────┬─────────┬──────────────┬──────────────┬────────────────┐${NC}"
    echo -e "${BOLD}│ SDK     │ Workers │ Throughput   │ Success Rate │ Scaling Factor │${NC}"
    echo -e "${BOLD}├─────────┼─────────┼──────────────┼──────────────┼────────────────┤${NC}"

    local baseline_throughput=""

    while IFS=',' read -r sdk workers workflows iterations avg_ms min_ms max_ms throughput success_rate successes total; do
        # Skip header
        if [ "$sdk" == "sdk" ]; then
            continue
        fi

        # Calculate scaling factor
        if [ -z "$baseline_throughput" ]; then
            baseline_throughput="$throughput"
            scaling="1.00x (baseline)"
        else
            scaling=$(python3 -c "print(f'{float(${throughput}) / float(${baseline_throughput}):.2f}x')" 2>/dev/null || echo "N/A")
        fi

        printf "${BOLD}│${NC} %-7s ${BOLD}│${NC} %7s ${BOLD}│${NC} %12s ${BOLD}│${NC} %12s ${BOLD}│${NC} %14s ${BOLD}│${NC}\n" \
            "$sdk" "$workers" "${throughput} wf/s" "${success_rate}%" "$scaling"
    done < "${RESULTS_DIR}/results.csv"

    echo -e "${BOLD}└─────────┴─────────┴──────────────┴──────────────┴────────────────┘${NC}"

    # Scaling analysis for Rust SDK
    if [ "$SDK" = "rust" ] || [ "$SDK" = "both" ]; then
        echo ""
        echo -e "${CYAN}Scaling Analysis (Rust SDK):${NC}"

        local first_throughput=""
        local first_workers=""
        local last_throughput=""
        local last_workers=""

        while IFS=',' read -r sdk workers workflows iterations avg_ms min_ms max_ms throughput success_rate successes total; do
            if [ "$sdk" != "rust" ]; then
                continue
            fi

            if [ -z "$first_throughput" ]; then
                first_throughput="$throughput"
                first_workers="$workers"
            fi

            last_throughput="$throughput"
            last_workers="$workers"
        done < "${RESULTS_DIR}/results.csv"

        if [ -n "$first_throughput" ] && [ -n "$last_throughput" ] && [ "$first_workers" != "$last_workers" ]; then
            local expected_linear=$(python3 -c "print(f'{float(${first_throughput}) * (${last_workers} / ${first_workers}):.2f}')" 2>/dev/null || echo "N/A")
            local efficiency=$(python3 -c "print(f'{float(${last_throughput}) / float(${expected_linear}) * 100:.1f}')" 2>/dev/null || echo "N/A")

            echo "  • Baseline (${first_workers} worker):   ${first_throughput} wf/s"
            echo "  • Max tested (${last_workers} workers): ${last_throughput} wf/s"
            echo "  • Expected (linear):     ${expected_linear} wf/s"
            echo "  • Scaling efficiency:    ${efficiency}%"
            echo ""

            local eff_num=$(python3 -c "print(${efficiency})" 2>/dev/null || echo "0")
            if (( $(echo "$eff_num > 80" | bc -l 2>/dev/null || echo "0") )); then
                echo -e "  ${GREEN}✅ Excellent scaling efficiency${NC}"
            elif (( $(echo "$eff_num > 50" | bc -l 2>/dev/null || echo "0") )); then
                echo -e "  ${YELLOW}⚠️  Moderate scaling efficiency - some contention detected${NC}"
            else
                echo -e "  ${RED}❌ Poor scaling efficiency - investigate bottlenecks${NC}"
            fi
        fi
    fi

    echo ""
    echo -e "${CYAN}Results saved to:${NC}"
    echo "  • CSV: ${RESULTS_DIR}/results.csv"
    echo "  • Logs: ${RESULTS_DIR}/"
    echo ""

    # Save summary to file
    {
        echo "ORCHER Unified Benchmark Summary"
        echo "================================="
        echo "Date: $(date)"
        echo "Mode: $([ "$SYNC_MATCH_MODE" = true ] && echo "Sync-Match" || echo "Redis Streams")"
        echo "SDK: ${SDK}"
        echo "Worker counts: ${WORKER_COUNTS}"
        if [ "$AUTO_SCALE_WORKFLOWS" = true ]; then
            echo "Workflow scaling: AUTO (${WORKFLOWS_PER_WORKER} per worker)"
        elif [ -n "$WORKFLOWS_TOTAL" ]; then
            echo "Workflows: ${WORKFLOWS_TOTAL} (fixed total)"
        else
            echo "Workflows per test: ${WORKFLOW_COUNT}"
        fi
        echo "Iterations: ${ITERATIONS}"
        echo ""
        echo "Results:"
        cat "${RESULTS_DIR}/results.csv"
    } > "${RESULTS_DIR}/summary.txt"

    log_success "Benchmark complete! Summary saved to ${RESULTS_DIR}/summary.txt"
}

#==============================================================================
# Run Main
#==============================================================================

main
