# Component responsibilities

## Control plane

**FastAPI** handles auth context, request validation, API contracts, idempotency lookup, and reads/writes in a transaction. It should return an acceptance response promptly; it must not wait for training. **Do not** use FastAPI `BackgroundTasks` as the durable execution engine.

**PostgreSQL** is the authoritative job/attempt ledger. It holds state, request specs, tenant ownership, leases, idempotency keys, allocation and model references. Postgres does not replace execution workers.

**Job controller** runs separately from API processes. It leases queued work, submits to Ray, reconciles unknown outcomes, owns retry/backoff and cancellation, and updates status safely. MLP-011 adds model registration and reconciliation of desired deployment state; MLP-017 adds tenant caps.

## Execution plane

**Ray Core** supplies the task/actor exercises in MLP-006. The durable adapter in MLP-007 uses **Ray Jobs** to launch a driver on an independently running Ray cluster, using a stable submission identity. **Ray Train** orchestrates distributed PyTorch training inside that driver; the capstone starts CPU-only. **Ray Serve** runs versioned inference replicas independently from training throughput.

**MinIO/S3** stores checkpoints and model artifacts. Checkpoint restore must prove that model/optimizer/epoch state was restored correctly.

## Infrastructure layer

**Kubernetes / KubeRay** schedules Ray workloads and GPU requests. GPU resources require device plugin support, valid GPU nodes and scheduling rules. CPU manifests should be demonstrable on a local cluster; GPU-specific manifests can be validated without GPU hardware and documented as `DESIGN-ONLY`.

## Cross-cutting concerns

- Tenancy: identity -> tenant policy -> admission, quotas, artifact access and resource fairness.
- Observability: structured logs, Prometheus metrics, tracing correlation where useful.
- Cost: estimate allocated GPU-hours; allocation is not measured GPU utilization or cloud invoice.
- Security: workload identity, least privilege, API authentication and restricted artifact access.

## Contract between control and execution

Example minimal adapter interface:

```python
class TrainingExecutor(Protocol):
    async def submit(
        self,
        submission_id: str,
        spec: JobSpec,
        *,
        checkpoint_uri: str | None = None,
    ) -> None: ...
    async def status(self, submission_id: str) -> ExecutionStatus: ...
    async def cancel(self, submission_id: str) -> None: ...
```

The controller persists `submission_id` (`kusanagi-<attempt UUID>`, tied to one immutable job/attempt record), before calling `submit()`. All operations address that same identity on the recorded Ray cluster. A lost submit response must not require a newly returned ID to find the execution. Reuse the identity for reconciliation; allocate a new one only for a new attempt after confirmed failure.

`ExecutionStatus` is an observation, separate from `JobStatus`: `NOT_FOUND`, `UNKNOWN`, `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, or `CANCELLED`. `UNKNOWN` means the lookup could not establish the outcome; `NOT_FOUND` means the reachable target currently has no matching job. Neither alone proves that an in-flight submit cannot still arrive. A successful `cancel()` call acknowledges a stop request, not confirmed termination.

Do not copy Ray Jobs driver status directly into training status. A running driver can still be waiting for Train workers/resources. From MLP-009, require an attempt-scoped training-start signal before reporting `RUNNING`; until then return `PENDING` with the waiting reason. Track driver startup separately from training time. MLP-007's dummy workload uses its own start signal.

Ray Jobs supports a caller-supplied `submission_id`; duplicate submission raises an error rather than returning the previous result. The adapter must look up that identity and verify the existing execution instead of treating every submission error as a retryable training failure. See [Ray Jobs submit API](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/doc/ray.job_submission.JobSubmissionClient.submit_job.html).

MLP-007 implements this contract with a [Ray Jobs adapter](../06-engineering/ray-jobs.md): persisted target/session, dispatch admission/revocation, verified metadata and retained remote identities. The local adapter in MLP-004 follows it with controlled fake execution. A bare process-local Ray `ObjectRef` is not the persisted lookup contract. Keep cluster recreation/head failure outside the MVP recovery claim; see [reliability](reliability.md).
