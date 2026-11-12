<!-- banner -->
<p>
  <img alt="ORCHER Engine" src="./assets/banner.svg#gh-dark-mode-only" width="100%">
  <img alt="ORCHER Engine" src="./assets/banner-light.svg#gh-light-mode-only" width="100%">
</p>
<!-- /banner -->

<p align="center"><sub>Workflows that survive crashes: the durable execution engine.</sub></p>

<br />

<div>
  <a href="https://github.com/orcher-io/orcher/releases/latest"><img src="https://img.shields.io/github/v/release/orcher-io/orcher?style=flat-square&labelColor=0a0a0a&color=04B385&logo=github&logoColor=white&label=release" alt="release"></a>
  <a href="https://github.com/orcher-io/orcher/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/orcher-io/orcher/ci.yml?branch=main&style=flat-square&labelColor=0a0a0a&color=38BDF0&logo=github&logoColor=white&label=CI" alt="CI"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-04B385?style=flat-square&labelColor=0a0a0a" alt="Apache 2.0"></a>
</div>

<br />

ORCHER runs workflows written as ordinary async code in Rust, Python or
TypeScript, and journals every step in PostgreSQL. When a worker crashes or is
redeployed, the run resumes from its last completed step on another worker.

- <img height="14" src="https://octicons-col.vercel.app/sync/38BDF0"> **Durable execution**: a completed step is recorded once and never repeated when a run resumes
- <img height="14" src="https://octicons-col.vercel.app/code/38BDF0"> **Three SDKs**: [Rust](https://github.com/orcher-io/sdk-rust), [Python](https://github.com/orcher-io/sdk-py) and [TypeScript](https://github.com/orcher-io/sdk-ts), sharing one core
- <img height="14" src="https://octicons-col.vercel.app/database/38BDF0"> **PostgreSQL only**: one engine process and one database; no other service to run
- <img height="14" src="https://octicons-col.vercel.app/clock/38BDF0"> **Timers and schedules**: durable timers, cron schedules and delayed starts that outlive the worker
- <img height="14" src="https://octicons-col.vercel.app/broadcast/38BDF0"> **Events and child workflows**: send events to running workflows, and compose workflows from others
- <img height="14" src="https://octicons-col.vercel.app/stack/38BDF0"> **Task queues and namespaces**: route work to the workers that can do it, and keep environments or teams apart
- <img height="14" src="https://octicons-col.vercel.app/graph/38BDF0"> **Dashboard, REST API and metrics**: served by the engine on its HTTP port

<br />

### <img height="16" src="https://octicons-col.vercel.app/play/38BDF0"> Run it

The engine is published as a container image for linux/amd64 and linux/arm64.
It needs a PostgreSQL 14+ database and nothing else:

```bash
docker network create orcher
docker run -d --name orcher-postgres --network orcher \
  -e POSTGRES_USER=orcher -e POSTGRES_PASSWORD=orcher -e POSTGRES_DB=orcher \
  postgres:17-alpine
docker run -d --name orcher --network orcher \
  -p 127.0.0.1:50051:50051 -p 127.0.0.1:8080:8080 \
  -e DATABASE_URL=postgresql://orcher:orcher@orcher-postgres:5432/orcher \
  -e ORCHER_INSECURE=true \
  ghcr.io/orcher-io/orcher:latest
curl -fsS localhost:8080/health/ready && echo ready
```

Point an SDK at `http://localhost:50051`, and open
[http://localhost:8080](http://localhost:8080) for the dashboard. More on the
image, its settings and its licences: [docker/engine/README.md](docker/engine/README.md).

> [!NOTE]
> `ORCHER_INSECURE=true` runs without authentication, which suits a laptop. The
> engine refuses to serve unauthenticated on an address other machines can reach
> unless you set it.

The [ORCHER CLI](https://github.com/orcher-io/cli) does the same in one command,
`orcher dev start --engine-version 0.7.0`, and drives a running engine from a
terminal.

<br />

### <img height="16" src="https://octicons-col.vercel.app/rocket/38BDF0"> Run a workflow

The [quickstart](https://github.com/orcher-io/quickstart) runs one order
workflow in Rust, Python and TypeScript against this image, then kills the
worker midway and shows the run resume. To write your own, pick an SDK:

| SDK | Repository | Install |
|-----|-----------|---------|
| Rust | [orcher-io/sdk-rust](https://github.com/orcher-io/sdk-rust) | `cargo add orcher-sdk` |
| Python | [orcher-io/sdk-py](https://github.com/orcher-io/sdk-py) | `pip install orcher-sdk` |
| TypeScript | [orcher-io/sdk-ts](https://github.com/orcher-io/sdk-ts) | `npm install @orcher/sdk` |

The SDKs share a core, [orcher-io/sdk-core](https://github.com/orcher-io/sdk-core),
and speak the API defined in [orcher-io/protos](https://github.com/orcher-io/protos).

<br />

### <img height="16" src="https://octicons-col.vercel.app/workflow/38BDF0"> Architecture

One process serves the gRPC API the SDKs and CLI use, and the REST API,
dashboard, health checks and metrics on its HTTP port. Workers connect to the
engine and poll it for work; the engine never calls them.

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

<br />

### <img height="16" src="https://octicons-col.vercel.app/tools/38BDF0"> Build from source

Needs a current stable Rust toolchain and PostgreSQL 14+.

```bash
git clone https://github.com/orcher-io/orcher.git
cd orcher

# Create the database and apply the schema
createdb orcher
DATABASE_URL='postgresql://localhost/orcher' cargo run --release -p orcher-storage --bin orcher-migrate -- run

# Start the engine: gRPC :50051, HTTP :8080
DATABASE_URL='postgresql://localhost/orcher' cargo run --release -p orcher-orchestrator
```

```bash
cargo test                # run the tests
cargo check --workspace   # check all crates
```

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

<br />

### <img height="16" src="https://octicons-col.vercel.app/plug/38BDF0"> REST API

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
| `POST /api/v1/executions/:id/reset` | Reset an execution to a journal entry (after 0.7.0) |
| `GET /api/v1/namespaces` | List namespaces |
| `POST /api/v1/namespaces` | Create namespace |
| `GET /api/v1/schedules` | List schedules |
| `POST /api/v1/schedules` | Create schedule |
| `GET /api/v1/workers` | List connected workers |
| `POST /api/v1/batch-operations` | Create batch operation |
| `GET /api/v1/api-keys` | List API keys |
| `POST /api/v1/api-keys` | Issue an API key |
| `DELETE /api/v1/api-keys/:id` | Revoke an API key |

<br />

### <img height="16" src="https://octicons-col.vercel.app/gear/38BDF0"> Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | PostgreSQL connection string (required) |
| `GRPC_BIND_ADDR` | `127.0.0.1:50051` | gRPC API address (`--grpc-addr`) |
| `HTTP_BIND_ADDR` | `127.0.0.1:8080` | REST API, health and metrics address (`--http-addr`) |
| `ORCHER_ORCHESTRATOR_CONFIG` | — | TOML config file (`--config`), e.g. `[security] auth_mode` |
| `ORCHER_UI_ENABLED` | `true` | `false` (or `--no-ui`) stops the engine serving the dashboard |
| `ORCHER_INSECURE` | unset | `true` serves without authentication on an address other machines can reach |

The orchestrator stores and delivers everything through PostgreSQL. Sync-match,
the opt-in dispatch path enabled with `ORCHER_USE_SYNC_MATCH=true`, hands work
to waiting workers in-process and also needs only PostgreSQL. `REDIS_URL` and
`ORCHER_USE_STREAMS` have no effect; the orchestrator logs a warning if they are
set.

<br />

### <img height="16" src="https://octicons-col.vercel.app/arrow-up/38BDF0"> Upgrading

Behaviour changes and migration steps for each release are in
[UPGRADING.md](UPGRADING.md); read it before moving an existing deployment to a
new release.

<br />

### <img height="16" src="https://octicons-col.vercel.app/heart/38BDF0"> Contributing

Issues and pull requests are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md).
To report a vulnerability, see [SECURITY.md](SECURITY.md), and please do not
open a public issue for one.

<br />

### <img height="16" src="https://octicons-col.vercel.app/law/38BDF0"> License

Licensed under the [Apache License, Version 2.0](LICENSE). That includes the
prebuilt engine images at `ghcr.io/orcher-io/orcher`.
