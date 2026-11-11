# orcher-core

The engine itself: the gRPC services workers and clients call, workflow and task execution, the journal, timers, retries, authentication and tenancy.

Part of the Orcher engine workspace; it is built into the `orchestrator` binary and is not published on its own.

## What is where

- `grpc`: the gRPC services (workflow, execution, worker, namespace, query, actor) and their authentication layer
- `workflows`: starting runs, applying completions, the journal, child workflows and events
- `engine`: dispatch, the outbox, timers, and handing activations to polling workers
- `worker`: worker registration, heartbeats and liveness
- `auth`: who is calling, which organization they act for and what they may do: API keys and OIDC
- `tenancy`: keeping one organization's runs, queues and workers from another's
- `retries`: retry policies and timeouts
- `services`: background services: claim cleanup, schedules, timers, task dispatch
- `jobs`: periodic maintenance: metrics aggregation and data retention

## Tests

```bash
cargo test -p orcher-core
```

Tests that need PostgreSQL are marked `#[ignore]`. They read the database
from `TEST_DATABASE_URL` or, in older tests, `DATABASE_URL`; set both:

```bash
export TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/orcher_test
export DATABASE_URL=$TEST_DATABASE_URL
cargo test -p orcher-core -- --ignored --test-threads=1
```

CI runs the ones listed in `scripts/ci.sh` (`guards`), each with what it proves.
