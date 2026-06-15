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
`--set tls.enabled=true --set tls.secretName=atomic-tls` (build the image with the
`tls` feature).
