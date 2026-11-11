# Kubernetes Deployment for ORCHER Orchestrator Service

This directory contains Kubernetes manifests for deploying the ORCHER Orchestrator service.

## Prerequisites

- Kubernetes cluster (v1.24+)
- kubectl configured to access your cluster
- Namespace `orcher` created
- Secret `orcher-secrets` with database credentials
- A PostgreSQL server — this is **not** part of
  this bundle. Size Postgres `max_connections` per
  [Sizing Postgres connections](#sizing-postgres-connections) before scaling up.

## High availability

`replicas: 3` only forms a **single cluster** because the Deployment sets
`ORCHER_CLUSTER_ENABLED=true` and gives each pod a unique `ORCHER_NODE_ID` (the
pod name, via the Downward API). Together these enable shard ownership and
generation-fenced writes, so losing a pod hands its work to a survivor without
double-execution.

Do not remove either setting: without `ORCHER_CLUSTER_ENABLED` the replicas run
as independent standalone engines, and without a per-pod `ORCHER_NODE_ID` they
all register under the same identity (the default is the bind address, which is
identical on every pod).

Each pod also advertises `ORCHER_ADVERTISE_ADDR` (its pod IP and gRPC port).
The Service spreads a worker's calls over every pod, so a workflow completion
often reaches a pod that does not own the workflow; that pod relays it to the
owner at the owner's advertised address, and the owner checks the worker's own
credentials. Pods must therefore be able to reach each other on port 50051:
wherever a NetworkPolicy applies to these pods, it must allow ingress on 50051
from the other orchestrator pods, not only from workers. An IPv6 pod IP is
bracketed automatically, so `$(POD_IP):50051` works on single-stack IPv6.
Without an advertised address a pod neither relays nor is relayed to, and
behaves as it did before relaying existed; a loopback or wildcard bind address
is never advertised unless set explicitly. `ORCHER_CLUSTER_FORWARDING=false`
turns relaying off.

With TLS on (`security.tls`), pods dial each other over TLS and verify the
peer against `security.tls.ca_cert_path`. Without a CA configured, relaying is
turned off at startup with an error in the log, and the pod keeps the older
ownership behaviour. Pods are dialed by IP, which a certificate rarely names:
set `ORCHER_FORWARD_TLS_DOMAIN` to the name the engines' certificates carry
(for example the Service's DNS name), and that name is verified instead.
`ORCHER_FORWARD_TIMEOUT_MS` caps how long one relay may wait for the owner
(default 30000).

Failover is verified by `infra/k8s/failover-test/`, which deploys **this bundle**
and asserts exactly-once through an orchestrator pod kill.

### Sizing Postgres connections

Each pod opens up to `ORCHER_PG_MAX_CONNECTIONS` (default in this bundle: `25`).
Keep `replicas × ORCHER_PG_MAX_CONNECTIONS` plus headroom for migrations and
admin sessions **below** the server's `max_connections`, otherwise a restarting
or newly scheduled pod fails to connect and crash-loops. With the shipped values
(3 × 25 = 75), a server configured for `max_connections=100` is already tight —
raise it (e.g. `200`) before scaling replicas or the HPA maximum.

## Schema migrations

The orchestrator does **not** migrate on startup in production — it fails fast if
the schema is behind. `migrate-job.yaml` runs the migrations using the same image
(it ships the `orcher-migrate` binary) and is included in the kustomization, so
`kubectl apply -k .` applies the schema before the pods become healthy. The Job
sets `ttlSecondsAfterFinished`, so it removes itself after completion and can be
re-applied on the next upgrade.

Run migrations **before or with** each upgrade; on a fresh database the pods will
crash-loop until the Job succeeds.

## Quick Start

### 1. Create Namespace

```bash
kubectl create namespace orcher
```

### 2. Create Secrets

```bash
kubectl create secret generic orcher-secrets \
  --namespace=orcher \
  --from-literal=database-url='postgresql://user:password@postgres:5432/orcher'
```

### 3. Deploy with Kustomize

```bash
kubectl apply -k .
```

Or deploy individual manifests:

```bash
kubectl apply -f configmap.yaml
kubectl apply -f rbac.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl apply -f hpa.yaml
kubectl apply -f pdb.yaml
```

## Resources

### ConfigMap (`configmap.yaml`)

- Contains TOML configuration for the orchestrator service
- Mounted at `/etc/orchestrator/config.toml`

### Deployment (`deployment.yaml`)

- 3 replicas by default
- Resource limits: 2 CPU, 2Gi memory
- Health checks configured
- Pod anti-affinity for high availability

### Service (`service.yaml`)

- **orchestrator**: Standard ClusterIP service
  - gRPC port: 50051
  - HTTP port: 8080
- **orchestrator-headless**: Headless service for direct pod access

### RBAC (`rbac.yaml`)

- ServiceAccount for the orchestrator pods
- Role with minimal permissions
- RoleBinding to associate them

### HorizontalPodAutoscaler (`hpa.yaml`)

- Min replicas: 3
- Max replicas: 10
- Scales based on CPU (70%) and memory (80%) utilization

### PodDisruptionBudget (`pdb.yaml`)

- Ensures at least 2 pods are always available
- Protects against voluntary disruptions

## Verification

Check deployment status:

```bash
kubectl get pods -n orcher -l app=orchestrator
kubectl get svc -n orcher -l app=orchestrator
kubectl get hpa -n orcher
```

Check logs:

```bash
kubectl logs -n orcher -l app=orchestrator -f
```

Test health endpoints:

```bash
# Port forward to a pod
kubectl port-forward -n orcher service/orchestrator 8080:8080

# Test in another terminal
curl http://localhost:8080/health
curl http://localhost:8080/health/ready
curl http://localhost:8080/health/live
curl http://localhost:8080/metrics
```

Test gRPC endpoint:

```bash
# Port forward gRPC port
kubectl port-forward -n orcher service/orchestrator 50051:50051

# Test with grpcurl (if installed)
grpcurl -plaintext localhost:50051 list
```

## Configuration

### Environment Variables

Override configuration via environment variables:

- `DATABASE_URL` - PostgreSQL connection string (from secret)
- `LOG_LEVEL` - Logging level (debug, info, warn, error)
- `LOG_FORMAT` - Log format (json, text)
- `GRPC_BIND_ADDR` - gRPC server address
- `HTTP_BIND_ADDR` - HTTP server address
- `ORCHER_INSECURE` - `true` serves the API (gRPC, and REST on the HTTP port) without authentication on a non-loopback address (set in `deployment.yaml` for the in-cluster network; remove it once `[security] auth_mode` is set)

### Scaling

Manual scaling:

```bash
kubectl scale deployment orchestrator -n orcher --replicas=5
```

The HPA will automatically scale between 3-10 replicas based on load.

## Monitoring

### Prometheus Metrics

The service exposes Prometheus metrics at `/metrics`:

- `orchestrator_tasks_submitted_total` - Total tasks submitted
- `orchestrator_tasks_completed_total` - Total tasks completed
- `orchestrator_tasks_failed_total` - Total tasks failed
- `orchestrator_workers_registered_total` - Total workers registered
- `orchestrator_workers_active` - Current active workers

Scrape configuration is added via pod annotations:

```yaml
prometheus.io/scrape: "true"
prometheus.io/port: "8080"
prometheus.io/path: "/metrics"
```

## Troubleshooting

### Pods not starting

Check events:

```bash
kubectl describe pod -n orcher -l app=orchestrator
```

Check logs:

```bash
kubectl logs -n orcher -l app=orchestrator --tail=100
```

### Connection issues

Verify service endpoints:

```bash
kubectl get endpoints -n orcher orchestrator
```

Test connectivity from another pod:

```bash
kubectl run -it --rm debug --image=busybox --restart=Never -n orcher -- sh
# Inside the pod:
wget -qO- http://orchestrator:8080/health
```

### Database connection failures

Verify secret exists:

```bash
kubectl get secret orcher-secrets -n orcher
kubectl get secret orcher-secrets -n orcher -o yaml
```

Test database connectivity:

```bash
kubectl run -it --rm psql --image=postgres:15 --restart=Never -n orcher -- \
  psql "postgresql://user:password@postgres:5432/orcher"
```

## Cleanup

Remove all resources:

```bash
kubectl delete -k .
```

Or remove individually:

```bash
kubectl delete deployment orchestrator -n orcher
kubectl delete service orchestrator orchestrator-headless -n orcher
kubectl delete configmap orchestrator-config -n orcher
kubectl delete serviceaccount orchestrator -n orcher
kubectl delete role orchestrator -n orcher
kubectl delete rolebinding orchestrator -n orcher
kubectl delete hpa orchestrator -n orcher
kubectl delete pdb orchestrator -n orcher
```

## Production Considerations

1. **Secrets Management**: Use a secrets management solution like:

   - Sealed Secrets
   - External Secrets Operator
   - HashiCorp Vault

2. **Image Registry**: Push images to a private registry:

   ```bash
   docker tag orcher/orchestrator:latest your-registry/orcher/orchestrator:v0.1.0
   docker push your-registry/orcher/orchestrator:v0.1.0
   ```

   Update `kustomization.yaml`:

   ```yaml
   images:
     - name: orcher/orchestrator
       newName: your-registry/orcher/orchestrator
       newTag: v0.1.0
   ```

3. **Resource Limits**: Adjust based on actual usage:

   ```yaml
   resources:
     requests:
       memory: "1Gi"
       cpu: "1000m"
     limits:
       memory: "4Gi"
       cpu: "4000m"
   ```

4. **Monitoring**: Set up alerts for:

   - Pod restart rate
   - Memory/CPU usage
   - Request latency
   - Error rates

5. **Network Policies**: Add network policies to restrict traffic:

   ```yaml
   apiVersion: networking.k8s.io/v1
   kind: NetworkPolicy
   metadata:
     name: orchestrator
   spec:
     podSelector:
       matchLabels:
         app: orchestrator
     policyTypes:
       - Ingress
       - Egress
   ```

## Support

For issues or questions, refer to the main ORCHER documentation or create an issue in the repository.
