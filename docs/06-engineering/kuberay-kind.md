# Run Ray batch and Serve on kind

MLP-015 exercises the CPU execution plane with two small Ray clusters on one
Kubernetes node. The [RayJob](../../deploy/kind/ray-job.yaml) submits a
two-row scoring job to an existing [RayCluster](../../deploy/kind/ray-cluster.yaml).
The [RayService](../../deploy/kind/ray-service.yaml) owns a separate RayCluster
and exposes the same deterministic predictor through a Kubernetes Service.
The [CPU platform test](cpu-pipeline.md) covers the FastAPI, PostgreSQL,
training, S3, and model-registration workflow on the local runtime.

## Run and validate

Use Docker with at least 4 CPUs and 8 GB memory allocated to the Linux VM,
kind v0.31.0, kubectl compatible with Kubernetes v1.35, Helm 3 and Python 3.12.
Use CLI builds that match your host architecture.
The script needs access to the KubeRay Helm repository and the image registries
for kind, Ray and the operator. It pins the kind and Ray image digests and
installs KubeRay chart 1.6.2. The Ray image supports Linux AMD64 and ARM64.
See the [kind release](https://github.com/kubernetes-sigs/kind/releases/tag/v0.31.0)
and the [KubeRay operator release](https://github.com/kuberay-project/kuberay/releases/tag/v1.6.2).

```bash
scripts/test-kuberay-kind.sh
```

Select Docker Desktop explicitly if another Docker context is active.

```bash
KUSANAGI_DOCKER_CONTEXT=desktop-linux scripts/test-kuberay-kind.sh
```

The script creates an isolated kubeconfig and a disposable kind cluster.
It waits for the operator and CRDs, then uses strict server-side dry-run
for the Kustomize output and the RayJob. The RayService controller parses the
embedded Serve configuration when it starts the application.
The script checks that the job reaches `SUCCEEDED` / `Complete` and the
RayService reaches `Ready`. It sends an HTTP request to the Serve ClusterIP
from the batch head Pod, checks HTTP `200` and compares both results with
`[1.0, 3.0]`. It also sends a batch that would overflow the predictor and
checks HTTP `422`. The `KUBERAY_BATCH` and `KUBERAY_PREDICT` records include the
actual worker Pod names and model version. The script prints the topology
before teardown.

## Inspect the resources

For interactive inspection, use a dedicated kind context and the same setup
sequence as the [script](../../scripts/test-kuberay-kind.sh). These commands
assume a kind cluster named `kusanagi-inspect` and its isolated kubeconfig.

```bash
kubectl --context kind-kusanagi-inspect -n kusanagi-nerv \
  get rayclusters,rayjobs,rayservices,pods,services -o wide
kubectl --context kind-kusanagi-inspect -n kusanagi-nerv describe rayjob cpu-batch-score
kubectl --context kind-kusanagi-inspect -n kusanagi-nerv describe rayservice cpu-predictor
kubectl --context kind-kusanagi-inspect -n kusanagi-nerv get events --sort-by=.lastTimestamp
kubectl --context kind-kusanagi-inspect -n kusanagi-nerv \
  port-forward svc/cpu-predictor-serve-svc 8001:8000
```

```bash
curl --fail --silent --show-error http://127.0.0.1:8001/predict \
  -H 'Content-Type: application/json' -d '{"instances":[[2,4,0,1],[0,0,4,0]]}'
```

The operator reconciles the RayJob and starts a Kubernetes submitter Job. The
Ray driver runs on the batch head and its tasks reserve one logical CPU on the
batch worker. Serve runs HTTP proxies on the head and the serving worker.
Its single model replica reserves one logical CPU on the serving worker.
Kubernetes admits both head Pods with 250m CPU requests and 1 CPU limits.
The heads advertise zero logical Ray CPUs and each worker advertises one.

## Ownership and failure domains

| Layer | Responsibility and failure boundary |
|---|---|
| Kubernetes scheduler and kubelet | Place Pods using CPU and memory requests. Enforce CPU and memory limits and report Pod scheduling and health. If a Pod is `Pending`, inspect Kubernetes events for resource or placement errors. |
| KubeRay operator | Reconcile the Ray custom resources into head/worker Pods, Services and submitter Jobs. Reconciliation resumes when the operator restarts. An existing Ray process can continue during operator downtime. |
| Ray runtime | Schedule tasks and actors using logical resources within the available Ray Pods. A lost worker can take its tasks, actors and local object state with it. Application retry and checkpoint behavior need their own policy. |
| Ray head and Serve | The head hosts GCS and the dashboard. A recreated head without persistent GCS state is a new runtime and can lose job metadata. The demo offers one serving replica. |

Ray Serve can scale model replicas based on request demand. The Ray autoscaler
can scale worker Pods based on pending logical resource demand when enabled
in KubeRay. A Kubernetes node autoscaler can add nodes when Pods cannot be
placed. The demo uses fixed workers and one kind node. Keeping these loops
distinct makes it possible to diagnose whether a wait comes from Serve, Ray
placement, or Kubernetes placement.
See the [RayCluster configuration guide](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/config.html),
[RayJob quickstart](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html),
and [RayService quickstart](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayservice-quick-start.html).

## Images, identity and storage

- The demo mounts its short Python files from a read-only, content-hashed
  ConfigMap. A release image for the platform should bake in the locked
  package and the required Ray/PyTorch dependencies, with one Python/Ray
  version across all Pods. Pin the resulting image by digest.
- The operator watches the demo namespace. Workload and submitter Pods use
  a dedicated ServiceAccount without mounted Kubernetes tokens. The head
  dashboard and Serve endpoint are internal Services in a trusted local
  cluster. A shared environment needs an explicit network and authentication
  design for both.
- The object store uses an in-memory `emptyDir` at `/dev/shm` and the demo
  needs no persistent volume or credentials. For the full platform, keep
  the job ledger in PostgreSQL and checkpoints/artifacts in S3, scoped with
  workload identities or Kubernetes Secrets where appropriate. Store
  endpoints and bucket names as configuration. Pod-local files and Ray
  object memory expire with the Pod.
- The platform controller currently uses the local Ray target identity and
  authenticated dashboard. A Kubernetes target and its reconciliation
  identity still need design. Keep one owner for submission and retry per job
  and one owner for the desired Serve config per cluster. See
  [Ray Jobs](ray-jobs.md) and [model deployment](model-deployment.md).
