# orcher-cluster

Cluster membership and shard ownership: which engine node owns which shard of the workflow ids, and the fencing that keeps a node that lost a shard from writing to it. Others depend on this crate; it depends on neither `orcher-core` nor `orcher-dispatch`.

Part of the Orcher engine workspace; it is built into the `orchestrator` binary and is not published on its own.

## What is where

- `shard`: how a workflow id maps to a shard, and contiguous-range ownership
- `membership`: live views of the cluster's nodes and the shard map
- `fencing`: this node's record of the shards it holds, and the generation token that guards writes
- `store`: the database-backed membership and fencing store (`DbCluster`)
- `routing`: which database a shard's rows live in
- `address`: whether an address a node registered is one its peers can dial
- `traits`: the seam `orcher-dispatch` and `orcher-core` consume

## Tests

```bash
cargo test -p orcher-cluster
```

Tests that need PostgreSQL are marked `#[ignore]`. They read the database
from `TEST_DATABASE_URL` or, in older tests, `DATABASE_URL`; set both:

```bash
export TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/orcher_test
export DATABASE_URL=$TEST_DATABASE_URL
cargo test -p orcher-cluster -- --ignored --test-threads=1
```

CI runs the ones listed in `scripts/ci.sh` (`guards`), each with what it proves.
