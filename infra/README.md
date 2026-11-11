# infra

Deployment and infrastructure-test assets for orcher.

- **`k8s/deploy/`** — production Kubernetes manifests (kustomize) for the
  orchestrator. Supply `orcher-secrets` (database-url, redis-url) and a Postgres
  + Redis; see `k8s/deploy/README.md`.
- **`k8s/failover-test/`** — an on-demand pod-kill failover correctness gate
  (k3d + in-cluster load, asserts exactly-once through an orchestrator pod loss).
  See `k8s/failover-test/README.md`. Not an every-PR gate; run it for changes to
  the dispatch / completion / cluster / journal paths.
