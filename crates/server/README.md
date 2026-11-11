# orcher-server

The engine's REST API. The `orchestrator` binary serves it on its HTTP address, and its handlers call the gRPC services over an in-process connection, behind the same authentication as the gRPC port.

Part of the Orcher engine workspace; it is built into the `orchestrator` binary and is not published on its own.

## What is where

- `api`: the REST routes and handlers (`api/rest/v1`)
- `middleware`: authentication and API versioning
- `grpc`: the clients the handlers call the gRPC services with
- `state`: shared state for the handlers
- `server`: building the router

## Tests

```bash
cargo test -p orcher-server
```

Tests that need PostgreSQL are marked `#[ignore]`. They read the database
from `TEST_DATABASE_URL` or, in older tests, `DATABASE_URL`; set both:

```bash
export TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/orcher_test
export DATABASE_URL=$TEST_DATABASE_URL
cargo test -p orcher-server -- --ignored --test-threads=1
```

CI runs the ones listed in `scripts/ci.sh` (`guards`), each with what it proves.
