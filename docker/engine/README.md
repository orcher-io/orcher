# ghcr.io/orcher-io/orcher — the engine's developer image

The orchestrator and `orcher-migrate`, for linux/amd64 and linux/arm64, under
the Apache License 2.0 ([LICENSE](../../LICENSE), also at `/LICENSE` in the
image). Deployments keep
using [`services/orchestrator/Dockerfile`](../../services/orchestrator/Dockerfile);
this image is the one developers pull.

## Run it

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

Point an SDK at `http://localhost:50051`, namespace `default`, and open
[http://localhost:8080](http://localhost:8080) for the dashboard: workflows,
schedules, workers and namespaces. It asks for an API key only when
authentication is on.

On start the entrypoint applies migrations to the database `DATABASE_URL`
names, which must exist (`POSTGRES_DB` creates it in the postgres image),
retrying for up to `ORCHER_DB_WAIT_SECS` while Postgres starts, then execs the
orchestrator. Other commands run as given:
`docker run --rm -e DATABASE_URL=... ghcr.io/orcher-io/orcher orcher-migrate status`.

## Environment

| Variable | Default in the image | Meaning |
|---|---|---|
| `DATABASE_URL` | — (required) | PostgreSQL 14+ connection URL. |
| `GRPC_BIND_ADDR` | `0.0.0.0:50051` | gRPC API the SDKs and CLI connect to. |
| `HTTP_BIND_ADDR` | `0.0.0.0:8080` | The REST API (`/api/v1`), `/health`, `/health/ready`, `/health/live`, `/metrics`, and the dashboard at every other path. |
| `ORCHER_UI_ENABLED` | `true` | `false` serves the API without the dashboard (as does `orchestrator --no-ui`). |
| `LOG_LEVEL` | `info` | `trace` … `error`. Logs go to stdout. |
| `LOG_FORMAT` | `compact` | `compact`, `pretty` or `json`. |
| `ORCHER_AUTO_MIGRATE` | `true` | `false` skips the migration step at start. |
| `ORCHER_DB_WAIT_SECS` | `60` | How long the entrypoint retries migrations while Postgres starts. |
| `ORCHER_PG_MAX_CONNECTIONS` / `ORCHER_PG_MIN_CONNECTIONS` | `20` / `2` | Connection pool bounds. |
| `ORCHER_DEFAULT_HEARTBEAT_TIMEOUT_SECS` | engine default | Heartbeat timeout for tasks that set none. |
| `ORCHER_TIMEOUT_WATCHER_POLL_INTERVAL_SECS` / `ORCHER_TIMEOUT_WATCHER_GRACE_SECS` | engine default | Tighten to see task timeouts within seconds in tests. |
| `ORCHER_WORKFLOW_RETRY_POLL_INTERVAL_SECS` | `30` | How often the workflow retry scheduler looks for retries that are due. A scheduled retry wakes the scheduler itself; this poll is the backstop. |
| `ORCHER_GRPC_MAX_MESSAGE_BYTES` | `33554432` (32 MiB) | Largest gRPC message the engine sends or accepts. |
| `ORCHER_PAYLOAD_MAX_BYTES` | `8388608` (8 MiB) | Largest single payload (workflow input or result, task input or result, event payload); larger ones are refused with an error naming them. Must be below the message limit. |
| `ORCHER_ORCHESTRATOR_CONFIG` | unset | Path of a TOML config file (mount it in). Unset, built-in defaults apply. |
| `ORCHER_INSECURE` | unset | `true` serves the API without authentication on `GRPC_BIND_ADDR` and `HTTP_BIND_ADDR`. Required when authentication is off, because the container listens on every interface. |

The defaults are a single node with authentication off (`auth_mode = "none"`).
The container listens on every interface, and whether that reaches beyond
your machine is decided by how its ports are published, which the engine
cannot see; so with authentication off it refuses to start unless
`ORCHER_INSECURE=true` says that is intended. Publish the ports on localhost
only, as above, or turn authentication on (`[security] auth_mode = "api-key"`
in a config file) to serve other machines. `orcher dev start` does all of
this for you. Authentication,
clustering and sharding are configured through the config file and the
`ORCHER_CLUSTER_*` / `ORCHER_SHARD*` variables, which a developer image does
not need.

### The first API key

With `auth_mode = "api-key"` every call needs a key, including the one that
would issue the first. Issue it from the engine's own command line, which
writes it straight to the database:

```bash
docker exec orcher orchestrator api-key create --name admin --scope '*'
```

The key alone is printed on stdout, once; it is not stored. On a multi-tenant
deployment (`tenancy = "multi"`) add `--organization <id>`. Further keys are
issued with that one over the REST API (`POST /api/v1/api-keys`).
`orchestrator api-key list` and `orchestrator api-key revoke <id>` list and
revoke keys the same way; a revoked key is refused at once by every running
engine.

The container runs as uid/gid 10001 (`orcher`) and has a `HEALTHCHECK` on
`/health/ready`.

## Licenses

The engine is licensed under the Apache License 2.0
([LICENSE](../../LICENSE)), shipped as `/LICENSE`. The third-party code in it
stays under its own licenses: `/THIRD_PARTY_NOTICES` lists every crate compiled
into `orchestrator` and `orcher-migrate`, with its full license text and
where to get its source, followed by the Rust standard library's notices;
`/NOTICE` says where everything else is: the dashboard in
`/usr/share/orcher/ui`, Apache-2.0, with its own `LICENSE` and
`THIRD_PARTY_NOTICES` for the packages bundled into it, and the Debian
packages' own `/usr/share/doc/*/copyright`.

```bash
docker run --rm ghcr.io/orcher-io/orcher cat /THIRD_PARTY_NOTICES | less
```

`/THIRD_PARTY_NOTICES` is generated from `Cargo.lock` by every image build
([`third-party-notices.sh`](third-party-notices.sh), with
[cargo-about](https://github.com/EmbarkStudios/cargo-about)), not committed,
so it always describes the binaries it ships with. That build fails if any
dependency is licensed only under terms outside the allow-list in
[`about.toml`](about.toml) (permissive licenses, plus MPL-2.0), so a GPL,
AGPL or unlicensed crate cannot reach a published image unnoticed.

## How it is built

[`build-binaries.sh`](build-binaries.sh) compiles both architectures on the
build machine itself and cross-links them with zig (cargo-zigbuild) against
glibc 2.28, so neither is a Rust build under emulation or inside a Docker VM,
and generates `THIRD_PARTY_NOTICES` beside them. It also fetches the dashboard
release [`ui.lock`](ui.lock) pins ([`fetch-ui.sh`](fetch-ui.sh)), refused unless
its SHA-256 matches, and the image workflow checks that
[orcher-io/ui](https://github.com/orcher-io/ui)'s release workflow built it;
moving to another dashboard release is a change to `ui.lock`.
[`Dockerfile`](Dockerfile)
then packages them onto `debian:bookworm-slim` without running anything on the
target platform. Published by
[`engine-image.yml`](../../.github/workflows/engine-image.yml):

```bash
docker/engine/build-binaries.sh
docker buildx build -f docker/engine/Dockerfile \
  --platform linux/amd64,linux/arm64 -t orcher-engine:dev .
```
