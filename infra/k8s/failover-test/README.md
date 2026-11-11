# orcher Kubernetes pod-kill failover test

A correctness gate that proves orcher survives losing an orchestrator pod on
Kubernetes with **exactly-once** intact. It stands up a 3-node [k3d](https://k3d.io)
cluster (Postgres + 3 clustered orchestrators), drives workflows through
them from an **in-cluster** load harness, kills an orchestrator pod mid-flight,
and asserts every workflow completes exactly-once (0 duplicate, 0 stranded).

This is **not** part of the every-PR gate (it takes ~15–25 min and needs a
container runtime). Run it **on demand** for changes touching the dispatch,
completion, cluster, or journal paths — and it's the intended home for a future
nightly / manual-dispatch CI job. The fast every-PR guard remains
`cargo test` + the stress harness + `scripts/cluster-multinode-e2e.sh`.

## Prerequisites

- A container runtime — [`colima`](https://github.com/abiosoft/colima) recommended: `colima start --cpu 4 --memory 10`
- `docker`, `k3d`, `kubectl` on `PATH` (and `k9s` optional, to watch)
- The sibling `sdk-rust` repo checked out next to this one (the load harness image
  builds from it)

## Usage

```bash
cd infra/k8s/failover-test

./failover-test.sh                 # build images if missing, run once (graceful drain kill)
KILL_MODE=force ./failover-test.sh # SIGKILL crash instead of a graceful drain
RUNS=5 ./failover-test.sh          # repeat the kill scenario 5 times
SCENARIO=scale-up ./failover-test.sh    # add a pod mid-flight (handoff between live owners)
SCENARIO=seize ./failover-test.sh       # churn shard ownership under a live cluster
COUNT=500 DEADLINE=300 ./failover-test.sh
./failover-test.sh --rebuild       # force-rebuild the images first
./failover-test.sh --cleanup       # delete the cluster and exit
```

## Scenarios

| `SCENARIO` | disruption | what a green run means |
|---|---|---|
| `kill` (default) | delete a pod mid-flight | exactly-once survives losing a node |
| `scale-up` | add a pod mid-flight | shards hand off between *live* owners without duplicates |
| `seize` | reassign a live node's shards and bump their generation, repeatedly | exactly-once survives continuous ownership churn |

### What `seize` does not cover

It was written to show the write fence refusing a superseded write, and it does
not. Stated plainly so a green run is not mistaken for fence coverage:

* **Claim scoping sits upstream of the fence.** Once a node's shards are taken
  it stops being handed work, so it never reaches a write to refuse. A measured
  run reassigned ownership **4,460 times** during 400 workflows and logged
  **zero** fence refusals — while still completing 400/400 with 0 duplicates.
* The fence guards the narrower case of a write *already in flight* when
  ownership moves: a guard/commit window well under a millisecond. A shell
  harness cannot hit that reliably.

The fence's refusal behaviour is covered by the six live-database tests in
`crates/core/tests/write_fence_refuses_stale_owner.rs`, which construct the
superseded state directly. `seize` covers what those cannot — that the ownership
machinery survives churn without losing exactly-once.

## Images are rebuilt when the engine changes

The harness used to reuse a cached image unless given `--rebuild`. A run on
2026-08-16 passed against an image built two days earlier, before the write
fence was wired to the repository, and reported `shards acquired: 0` — which
reads like a dormant fence rather than a stale binary. It now rebuilds whenever
the Rust sources are newer than the image; `REUSE_IMAGES=true` opts out.

Exit code is `0` on pass, `1` on fail. The cluster is left running for inspection
(`k9s`, context `k3d-orcher-ha`); tear down with `--cleanup`.

## What "pass" means

- **Correctness (hard gate):** zero duplicate task executions. A single duplicate
  fails the run immediately — that is an exactly-once violation.
- **Liveness (soft):** all submitted workflows reach a terminal state within the
  deadline. A shortfall with **zero** duplicates is treated as a slow/undersized
  runner and the run is retried once before failing. Give the runtime enough CPU
  (Postgres runs in-cluster on emptyDir; the test DB has `fsync=off` to compensate).

## Layout

## It tests what we ship

`kustomization.yaml` is a **kustomize overlay of the production bundle**
(`../deploy`). The orchestrator Deployment, migration Job, Service, RBAC, HPA and
PDB all come from that base — the overlay only adds what a local cluster needs
(an in-cluster Postgres, an `orcher-secrets` Secret pointing at it, locally
built image tags, laptop-sized resource requests, and a pinned replica count so
the HPA can't scale mid-test).

This is deliberate: an earlier copy of the orchestrator manifest lived here and
drifted from the shipped one, which hid missing HA settings. Keeping the base
authoritative makes that class of drift impossible.

| File | Purpose |
| --- | --- |
| `failover-test.sh` | orchestrates the whole test (build → cluster → deploy → kill → assert) |
| `kustomization.yaml` | overlay: `../deploy` + test-only infra |
| `Dockerfile` | orchestrator + `orcher-migrate` image (built from the repo root) |
| `Dockerfile.stress` | in-cluster load/worker/ledger image (built from `sdk-rust`) |
| `manifests/00-namespace.yaml` | the `orcher` namespace |
| `manifests/10-postgres.yaml` | ephemeral Postgres (`max_connections=200`, `fsync=off` — test only) |
| `manifests/40-stress-job.yaml` | in-cluster stress harness; applied per-scenario by the script (it patches the workflow count) |

## Notes / gotchas

- **In-cluster load, not a host worker.** orcher dispatch is pull-based
  long-polling; a host worker over a NodePort can't sustain the long-polls
  (kube-proxy/conntrack idle + host↔VM hop). The harness runs as an in-cluster Job
  against the orchestrator `ClusterIP`.
- **DB pool sizing.** The default per-pod pool (max 80) exhausts Postgres's default
  `max_connections=100` with 3 pods; the manifests cap it (`ORCHER_PG_MAX_CONNECTIONS=15`)
  and raise `max_connections=200`.
- **Storage.** In-cluster Postgres on VM-backed `emptyDir` has slow fsync; the test
  DB disables it (`fsync=off`, safe for an ephemeral test DB, **never** in prod).
