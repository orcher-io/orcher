# orcher-dispatch

How work reaches workers: long polls, claims, prefetching, capacity and worker affinity. It depends on traits rather than on `orcher-core`'s concrete types.

Part of the Orcher engine workspace; it is built into the `orchestrator` binary and is not published on its own.

## What is where

- `polling`: long polls, claiming work and waking polls when work arrives
- `eager`: returning a workflow's next tasks in its completion response, sparing a poll
- `capacity`: permit-based worker capacity
- `routing`: task routing and worker affinity
- `queue`: partitioned task queues
- `sharding`: spreading work across engine instances

## Tests

```bash
cargo test -p orcher-dispatch
```

Tests that need PostgreSQL are marked `#[ignore]`. They read the database
from `TEST_DATABASE_URL` or, in older tests, `DATABASE_URL`; set both:

```bash
export TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/orcher_test
export DATABASE_URL=$TEST_DATABASE_URL
cargo test -p orcher-dispatch -- --ignored --test-threads=1
```

CI runs the ones listed in `scripts/ci.sh` (`guards`), each with what it proves.
