# orcher-storage

PostgreSQL persistence for the engine: the schema and its migrations, and the repositories every other crate reads and writes through. It also builds `orcher-migrate`, which applies and verifies migrations.

Part of the Orcher engine workspace; it is built into the `orchestrator` binary and is not published on its own.

## What is where

- `migrations`: the SQL migrations (`migrations/sql/postgres`), applied in order by `setup`
- `setup`: running and verifying migrations
- `repositories`: the repositories, one per domain (workflow executions, journal, tasks, workers, ...)
- `schema`: row and schema types
- `sharding`: routing a shard's rows to its database
- `stale_plan`: recovering from statements prepared against an older schema

## Tests

```bash
cargo test -p orcher-storage
```

Tests that need PostgreSQL are marked `#[ignore]`. They read the database
from `TEST_DATABASE_URL` or, in older tests, `DATABASE_URL`; set both:

```bash
export TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/orcher_test
export DATABASE_URL=$TEST_DATABASE_URL
cargo test -p orcher-storage -- --ignored --test-threads=1
```

CI runs the ones listed in `scripts/ci.sh` (`guards`), each with what it proves.

Apply the migrations to a database:

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/orcher \
  cargo run -p orcher-storage --bin orcher-migrate -- run
```
