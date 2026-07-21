# Deploying Atomic on Kubernetes

Atomic's same-binary + compile-time-dispatch model is a natural fit for Kubernetes:
**one image** plays both roles, and the `REGISTRY_FINGERPRINT` handshake guarantees
every pod from that image runs byte-identical task code — exactly the invariant a
rolling update of an immutable image gives you. Kubernetes is *not* a scheduler
backend Atomic integrates with; it just orchestrates the pods.

## Layout

- `Dockerfile` — multi-stage build of the single binary (override `--build-arg BIN=<pkg>`).
- `helm/atomic/` — Helm chart: worker `StatefulSet` + headless `Service`, driver
  `Job`/`Deployment`, `ConfigMap`, optional `HorizontalPodAutoscaler`, optional mTLS.

## How discovery works

Workers are a `StatefulSet` behind a **headless Service** whose DNS name resolves to
the A records of all worker pods. The driver is started with:

```
--workers dns:<worker-service>:<taskPort>
```

`AtomicApp` resolves that DNS name to the full worker set at startup and then
**re-resolves it periodically**, registering pods that appear when the StatefulSet
(or HPA) scales up. Pods that disappear are pruned by the existing heartbeat loop.
So autoscaling works end-to-end with no static worker list.

## Per-job worker allocation (`allocator: kube`)

The default (`allocator: static`) scopes each job over the standing worker pool above.
When jobs need *different* resources, set `allocator: kube`: the driver creates dedicated
worker pods per job via the Kubernetes API, sized to the job's `ResourceProfile`
(CPU/memory/count, GPU/extended resources, nodeSelector/affinity/tolerations), pins the
job's task placement to exactly those pods, and deletes them when the job finishes.

```bash
# Image must be built with the `k8s` feature (adds kube-rs).
helm install demo deploy/helm/atomic \
  --set image.repository=myrepo/atomic --set image.tag=0.1.0 \
  --set allocator=kube --set worker.enabled=false
```

With `allocator: kube` the chart also creates a `Role`/`RoleBinding` granting the driver
ServiceAccount pod-management rights, and injects the driver's pod identity
(`POD_NAME`/`POD_UID`) so worker pods carry an `OwnerReference` — Kubernetes
garbage-collects them if the driver dies. `worker.enabled: false` drops the standing
StatefulSet/HPA so the driver provisions everything on demand. From application code:

```rust
ctx.with_workers(
    ResourceProfile::new(6).with_cpu("4", "8").with_memory("8Gi", "16Gi"),
    |sc| sc.parallelize_typed(data, 6).map_task(Heavy).collect(),
).await?;
```

## Ad-hoc submission (`atomic submit-k8s`)

`helm install`/`helm upgrade` is for standing deployments — running one job still
means editing `driver.args` and re-releasing. `atomic submit-k8s` (`atomic-cli` built
with `--features k8s`) instead creates a single `batch/v1 Job` for one run and exits;
the `spark-submit` equivalent. It needs the same RBAC as `allocator: kube` above —
either a full `helm install --set allocator=kube`, or just the RBAC/ServiceAccount via
`--set driver.enabled=false`.

Two ways to get the driver binary into the pod — `--image` reuses the `docker
build`/`push` flow below; `--binary` skips it entirely by staging the compiled binary
to S3 and running it through `atomic-bootstrap`, a small generic fetch-and-exec image
published once by the project (never built per job, never built per user):

```bash
# --image: same as the Build & deploy flow below, wrapped in a one-off Job.
atomic submit-k8s --image myrepo/atomic:0.1.0 --namespace demo \
  --dynamic-workers --worker-image myrepo/atomic:0.1.0 -- --my-job-flag foo

# --binary: no image build at all.
atomic build --target x86_64-unknown-linux-musl
atomic submit-k8s --binary target/x86_64-unknown-linux-musl/release/my_app \
  --namespace demo --s3-bucket my-staging-bucket \
  --dynamic-workers --worker-image myrepo/atomic:0.1.0 -- --my-job-flag foo
```

`--dynamic-workers` makes the Kube allocator available to the submitted driver — the
job's own `ctx.with_workers(...)` call still decides the count/CPU/memory, exactly as
above. Omit it and pass `--workers dns:<svc>:<port>` as a trailing job arg instead to
point at an existing worker StatefulSet. `kubectl logs -f job/<name> -n <namespace>
-c driver` follows the run; there is no log-streaming built into the CLI itself.

## Build & deploy

```bash
# 1. Build the image for your app (a workspace package that calls AtomicApp::build()).
docker build -f deploy/Dockerfile --build-arg BIN=joins -t myrepo/atomic:0.1.0 .
docker push myrepo/atomic:0.1.0

# 2. Install the chart.
helm install demo deploy/helm/atomic \
  --set image.repository=myrepo/atomic --set image.tag=0.1.0 \
  --set worker.replicas=3

# 3. (optional) enable worker autoscaling.
helm upgrade demo deploy/helm/atomic --set autoscaling.enabled=true
```

The driver defaults to `driver.kind: Job` (a batch program that exits). Set it to
`Deployment` for a long-running driver service. Enable mTLS with
`--set tls.enabled=true --set tls.secretName=atomic-tls` (mTLS is built into the image —
no feature flag needed).
