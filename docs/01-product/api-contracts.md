# API contracts and error behavior

**API version:** `/v1`. This is a target contract and will evolve through ADRs.

## Create job

`POST /v1/jobs` with `Idempotency-Key` and JSON:

```json
{
  "model": "tiny-mlp",
  "dataset": "synthetic-v1",
  "epochs": 5,
  "resources": {"cpus": 2, "gpus": 0, "memory_gib": 2},
  "max_retries": 2
}
```

- Returns `202 Accepted`, `job_id`, `status: "queued"`, and `Location` pointing to the status route. From MLP-003 onward, commit the job and queue admission atomically before responding.
- Same tenant + key + canonical payload: same `job_id` and no extra execution.
- Same tenant + key + different canonical payload: `409 Conflict`.
- Invalid body: `422 Unprocessable Entity` (Pydantic validation).
- `tenant_id` is not a CreateJob body field; reject it as an extra field. Resolve tenant ownership from authenticated identity. Until MLP-017, a fixed test tenant or an explicitly test-only `X-Test-Tenant` header supplies the same internal tenant context; neither is production authentication.

Implement validation and create/get in MLP-002, persistence in MLP-003, and durable idempotency/cancellation in MLP-005. Partial milestones must record which target behaviors are not implemented yet.

## Job operations

| Method | Path | Semantics |
|---|---|---|
| `GET` | `/v1/jobs/{job_id}` | Tenant-scoped status, attempts, errors, waiting reason, checkpoint and result model references |
| `GET` | `/v1/jobs` | Paginated, stable ordering, tenant-scoped filters |
| `POST` | `/v1/jobs/{job_id}/cancel` | Requests best-effort cancellation; `CANCELLING` isn't `CANCELLED` |
| `POST` | `/v1/models/{model_id}/deploy` | Validate successful model artifact and request deployment |
| `GET` | `/v1/models/{model_id}` | Tenant-scoped model version, artifact metadata, deployment state and error |
| `POST` | `/v1/models/{model_id}/predict` | Proxy prediction or route to Ray Serve; include version |
| `GET` | `/healthz` | Liveness |
| `GET` | `/readyz` | Readiness of API dependencies (later milestone) |
| `GET` | `/metrics` | Prometheus exporter endpoint |

## Example status envelope

```json
{
  "job_id": "uuid",
  "status": "running",
  "attempt": 1,
  "requested_resources": {"cpus": 2, "gpus": 0, "memory_gib": 2},
  "checkpoint_uri": null,
  "artifact_uri": null,
  "result_model_id": null,
  "result_model_version": null,
  "waiting_reason": null,
  "error": null,
  "created_at": "2026-11-09T10:00:00Z",
  "updated_at": "2026-11-09T10:00:02Z"
}
```

## Retry and cancellation semantics

`max_retries` is the number of additional controller-managed attempts after the initial run: at most `1 + max_retries` attempt records. Reconciliation and repeat submission of the same attempt do not consume another retry. A confirmed attempt failure may be retried with backoff; an unknown outcome must first be reconciled. Runtime recovery ownership is specified in [reliability](../02-architecture/reliability.md#retry-ownership).

Cancel behavior:

- Unknown or other-tenant job: `404`.
- Safely undispatched job: atomically cancel and return `200` with `CANCELLED`.
- Work may exist remotely or a submission may still arrive: persist cancellation intent and return `202` with `CANCELLING`; repeated requests do not dispatch new work.
- Already terminal job: return `200` with its unchanged state. This is an idempotent API operation, not a terminal-state self-transition.

Cancellation can race with success. Guarded DB writes and authoritative attempt checks commit the observed outcome according to the [lifecycle guards](job-lifecycle.md#guards-for-persistent-execution).

## Model registration, deployment and prediction (MLP-011)

1. After validating the completed artifact, atomically finalize job success and create one model record for that source job. Repeated reconciliation returns the same model identity. The successful job view exposes `result_model_id`, `result_model_version` and `artifact_uri`; failed/cancelled jobs expose no result model.
2. `POST /v1/models/{model_id}/deploy` requests deployment of that immutable model/version. Return `202` with `model_id`, `version`, `deploy_state: "deploying"` and a `Location` for the model GET route. Repeating a request while deploying returns the same desired deployment; an already ready version returns `200`. Use a stable deployment identity and reconcile after controller restart.
3. `GET /v1/models/{model_id}` exposes `deploy_state` (`not_deployed`, `deploying`, `ready`, `failed`) and an error when deployment failed. A failed deployment may be requested again for the same model/version.
4. Deploy rejects a known but invalid/incomplete artifact with `409`; unknown or other-tenant model IDs return `404`. Models must belong to successful jobs and to the caller's tenant.
5. `POST /v1/models/{model_id}/predict` returns `200` with `model_id`, `version` and predictions. Return `409 model_not_ready` before deployment is ready, `422` for invalid input, and a controlled `503` for unavailable serving infrastructure. The public API preserves the tenant check before forwarding to Ray Serve.

This API flow is P0. The Python SDK wrapping it is optional P1; manual Ray commands are not the acceptance path for self-service deployment.

## SLO-oriented learning targets (not production commitments)

- Small local test: submission response p95 below 300 ms without executing model training in HTTP.
- Record and idempotency key survive API restart.
- From MLP-017, the default sample tenant cap is at most 2 active execution reservations, as defined in [the data model](../02-architecture/data-model.md#tenant-reservations-mlp-017). Queued work without a reservation and retry backoff do not consume a slot.
- Status and tenant data never leak across tenant boundaries.
- When GPU is unavailable, a GPU-required job remains waiting with a visible reason; it must **not** silently fall back to CPU.

See [requirements](requirements.md), [state machine](job-lifecycle.md) and [data model](../02-architecture/data-model.md).
