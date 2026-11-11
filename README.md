<h1 align="center">Orcher</h1>

<p align="center">
  Durable workflow orchestration platform built in Rust.
</p>

<p align="center">
  <a href="https://github.com/orcher-io/orcher/actions"><img src="https://img.shields.io/github/actions/workflow/status/orcher-io/orcher/ci.yml?branch=main" alt="CI"></a>
  <a href="https://github.com/orcher-io/orcher/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-blue" alt="License"></a>
</p>

---

Orcher is a workflow orchestration engine that makes distributed systems reliable. Workflows survive process restarts, deployments, and infrastructure failures — automatically.

## Features

- **Durable Execution** — Workflows resume exactly where they left off after any failure
- **Language-Native SDKs** — Write workflows in [Rust](https://github.com/orcher-io/sdk-rust), [Python](https://github.com/orcher-io/sdk-py), or [TypeScript](https://github.com/orcher-io/sdk-ts)
- **Scalable** — Horizontally scale workers and the control plane independently
- **Observable** — Full execution history, journal replay, real-time metrics
- **Task Queues** — Route work to specific workers by capability
- **Timers & Schedules** — Durable timers, cron schedules, delayed execution
- **Events** — Send events to running workflows from external systems
- **Child Workflows** — Compose complex pipelines from simpler workflows
- **Namespaces** — Isolate environments (dev/staging/prod) or teams
- **Batch Operations** — Cancel, terminate, reset or send an event to many workflows at once

## Quick Start

### Prerequisites

- Rust, a current stable toolchain
- PostgreSQL 14+

Postgres is the only datastore Orcher needs; see
[Redis is not needed](#redis-is-not-needed).

### 1. Build and migrate

```bash
git clone https://github.com/orcher-io/orcher.git
cd orcher

# Create the database and apply the schema
createdb orcher
DATABASE_URL='postgresql://localhost/orcher' cargo run --release -p orcher-storage --bin orcher-migrate -- run
```

### 2. Start ORCHER

One process serves everything: the gRPC API the SDKs and CLI use, and the REST
API, health checks and metrics on its HTTP port. No Redis instance is needed.

```bash
DATABASE_URL='postgresql://localhost/orcher' cargo run --release -p orcher-orchestrator   # gRPC :50051, HTTP :8080
```

To drive it from a terminal, install the [ORCHER CLI](https://github.com/orcher-io/cli)
(`brew install orcher-io/tap/orcher`, `cargo install orcher`, `pip install orcher`
or `npm install -g orcher`) and check it can reach the engine with `orcher status`.

### 3. Run a workflow

The examples run in the `examples` namespace, so create it first with
`orcher namespace create examples`. Then run the durable execution example from
the [Rust SDK's examples](https://github.com/orcher-io/sdk-rust/tree/main/examples):

```bash
git clone https://github.com/orcher-io/sdk-rust.git
cd sdk-rust
cargo run -p orcher-example-durable-execution -- --e2e
```

Or the Python SDK:

```bash
cd examples/sdk-py/durable-execution
pip install -e .
python -m src.main
```

## Architecture

```
     SDK clients, CLI                Scripts, HTTP clients
            │ gRPC :50051                  │ REST :8080
            └───────────────┬──────────────┘
                     ┌──────▼──────┐       ┌────────────┐
                     │   Engine    │──────▶│ PostgreSQL │
                     │             │       └────────────┘
                     └──────▲──────┘
                            │ gRPC: workers poll the engine for work
              ┌─────────────┼─────────────┐
        ┌─────┴────┐  ┌─────┴────┐  ┌─────┴────┐
        │ Worker 1 │  │ Worker 2 │  │ Worker N │
        │ (Rust)   │  │ (Python) │  │ (TS)     │
        └──────────┘  └──────────┘  └──────────┘
```

## Project Structure

```
orcher/
├── crates/
│   ├── common/         # Shared types, identifiers, error handling
│   ├── config/         # Configuration management
│   ├── storage/        # PostgreSQL repositories, migrations
│   ├── core/           # Orchestration engine, dispatch, polling, sharding
│   ├── dispatch/       # Task dispatch primitives (claimer, notifier, prefetch)
│   ├── cluster/        # Membership, shard ownership, write fencing
│   ├── events/         # Event bus for real-time streaming
│   ├── monitoring/     # Metrics, tracing, health checks
│   └── server/         # REST API and its middleware, served by the engine
├── services/
│   └── orchestrator/   # The engine: gRPC API, REST API, background work
└── examples/           # SDK examples (Rust, Python, TypeScript)
```

## SDKs

| SDK | Repository | Install |
|-----|-----------|---------|
| Rust | [orcher-io/sdk-rust](https://github.com/orcher-io/sdk-rust) | `cargo add orcher-sdk` |
| Python | [orcher-io/sdk-py](https://github.com/orcher-io/sdk-py) | `pip install orcher-sdk` |
| TypeScript | [orcher-io/sdk-ts](https://github.com/orcher-io/sdk-ts) | `npm install @orcher/sdk` |

SDKs share a common core: [orcher-io/sdk-core](https://github.com/orcher-io/sdk-core) (Rust, published as [`orcher-sdk-core`](https://crates.io/crates/orcher-sdk-core)) with language bindings via PyO3 (Python) and Neon (TypeScript).

Protocol definitions: [orcher-io/protos](https://github.com/orcher-io/protos)

## REST API

The engine serves a REST API on its HTTP port (8080 by default), behind the
same authentication as its gRPC API:

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/health/live` | Liveness probe |
| `GET /api/v1/health/ready` | Readiness probe |
| `GET /api/v1/executions` | List workflow executions |
| `GET /api/v1/executions/:id/status` | Get execution status |
| `GET /api/v1/executions/:id/journal` | Get execution journal |
| `POST /api/v1/executions/:id/cancel` | Cancel execution |
| `GET /api/v1/namespaces` | List namespaces |
| `POST /api/v1/namespaces` | Create namespace |
| `GET /api/v1/schedules` | List schedules |
| `POST /api/v1/schedules` | Create schedule |
| `GET /api/v1/workers` | List connected workers |
| `POST /api/v1/batch-operations` | Create batch operation |
| `GET /api/v1/api-keys` | List API keys |
| `POST /api/v1/api-keys` | Issue an API key |
| `DELETE /api/v1/api-keys/:id` | Revoke an API key |

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | PostgreSQL connection string (required) |
| `GRPC_BIND_ADDR` | `127.0.0.1:50051` | gRPC API address (`--grpc-addr`) |
| `HTTP_BIND_ADDR` | `127.0.0.1:8080` | REST API, health and metrics address (`--http-addr`) |
| `ORCHER_ORCHESTRATOR_CONFIG` | — | TOML config file (`--config`), e.g. `[security] auth_mode` |
| `ORCHER_INSECURE` | unset | `true` serves without authentication on an address other machines can reach |

### Redis is not needed

The orchestrator stores and delivers everything through PostgreSQL. Sync-match,
the opt-in dispatch path enabled with `ORCHER_USE_SYNC_MATCH=true`, hands work
to waiting workers in-process and also needs only PostgreSQL. `REDIS_URL` and
`ORCHER_USE_STREAMS` have no effect; the orchestrator logs a warning if they are
set.

## Development

```bash
# Run tests
cargo test

# Run benchmarks
bash scripts/benchmark.sh --quick --sdk rust

# Check all crates
cargo check --workspace
```

## Upgrading

Behaviour changes and migration steps for each release are in
[UPGRADING.md](UPGRADING.md); read it before moving an existing deployment to a
new release.

## Security

To report a vulnerability, see [SECURITY.md](SECURITY.md). Please do not open
a public issue for one.

## License

Orcher is licensed under the Apache License 2.0; see [LICENSE](LICENSE). That
includes the prebuilt engine images at `ghcr.io/orcher-io/orcher`.
