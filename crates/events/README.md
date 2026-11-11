# orcher-events

Events inside the engine: the domain event types, an in-process publish/subscribe bus, publishers, and replay from stored event streams. Optional parts are behind feature flags (`bus`, `publisher`, `metadata`, `json`, `replay`, `projections`, `storage`).

Part of the Orcher engine workspace; it is built into the `orchestrator` binary and is not published on its own.

## What is where

- `domain`: the domain event types, one enum per aggregate
- `bus`: the in-memory bus on tokio broadcast channels
- `publisher`: the `EventPublisher` trait and its implementations
- `replay`: the event store and stream replay
- `subscriptions`: subscriptions that resume from a checkpoint

## Tests

```bash
cargo test -p orcher-events
```

Tests that need PostgreSQL are marked `#[ignore]`. They read the database
from `TEST_DATABASE_URL` or, in older tests, `DATABASE_URL`; set both:

```bash
export TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/orcher_test
export DATABASE_URL=$TEST_DATABASE_URL
cargo test -p orcher-events -- --ignored --test-threads=1
```

CI runs the ones listed in `scripts/ci.sh` (`guards`), each with what it proves.
