# API contracts and error behavior

**API version:** `/v1`. This is a target contract and will evolve through ADRs.

## Implemented through MLP-005

Job creation, lookup and cancellation are implemented, plus FastAPI's `/docs` and `/openapi.json`. The app uses an injected PostgreSQL repository and the fixed `local-test-tenant` identity; memory is an explicit test adapter. This is a local learning configuration, not production authentication; no `X-Test-Tenant` header is enabled. Tests override the internal tenant dependency to verify isolation. `tenant_id` and all other extra request fields are rejected, including within `resources`.

- POST commits a queued job in PostgreSQL before returning `202` with a UUID and relative `Location`; it does not perform training or schedule a background task. GET returns the submitted specification, requested resources, status, timestamps and the result/error fields below. Admission initializes `attempt: 0` and null result/error/waiting fields. With the separate MLP-004 controller running, GET can already show an attempt or a later state, even on the first read; the acceptance response still reports queued admission.
- Optional `Idempotency-Key` deduplicates by tenant and canonical validated specification. Same payload returns the original `202` admission body/Location; a different payload returns `409 idempotency_conflict`. Keys are 1–255 visible ASCII characters, case-sensitive, with no whitespace or expiry. Duplicate headers are invalid. Unkeyed POSTs create separate jobs. Replayed admission does not change current job state; use GET for that state.
- Jobs survive API restarts; processes using the same database share the ledger. Migrations run explicitly before startup. MLP-004 adds simulated CPU dispatch and fenced observations. It creates no real model/artifacts; GPU requests stay queued. MLP-005 adds persisted retry budgets/backoff and guarded cancellation; GET also exposes `requested_cancel_at` and `retry_not_before`.
- Unknown and other-tenant UUIDs produce the same `404` envelope; a malformed UUID produces `422`.
- Database failures produce a sanitized `503 storage_unavailable`. An unconfirmed write is never acknowledged as `202`, but a lost response/connection during commit can leave the actual outcome unknown. There is no automatic write retry; repeat the same tenant/key/payload to resolve an uncertain commit. Repeating an unkeyed POST can still duplicate the logical job. See [storage failure semantics](../06-engineering/postgres.md#failure-behavior-and-remaining-scope).

### Input validation

These are explicit learning-project validation ceilings, not available capacity or tenant quotas. Actual scheduling remains a later concern. CPU and memory may be fractional; GPU count, epochs and retries must be integers. Booleans, numeric strings and non-finite numbers are rejected.

| Field | Accepted values | Default |
|---|---|---|
| `model`, `dataset` | 1–128 characters; ASCII letter/digit first, then letters/digits/`.`/`_`/`-` | Required |
| `epochs` | Integer, 1–10,000 inclusive | Required |
| `resources.cpus` | Finite number, greater than 0 and at most 64 | Required |
| `resources.gpus` | Integer, 0–8 inclusive | 0 |
| `resources.memory_gib` | Finite number, greater than 0 and at most 512 | Required |
| `max_retries` | Integer, 0–10 inclusive | 0 |

Identifier validation is syntactic; this milestone does not resolve a model/dataset catalog or accept executable code/URLs as identifiers.

### Error envelope

Documented `404`, `409`, `422` and `503` responses use `ErrorResponse`. Clients should use `error.code` and detail locations; human-readable validation messages can evolve with Pydantic.

```json
{
  "error": {
    "code": "validation_error",
    "message": "Request validation failed",
    "details": [
      {"location": ["body", "epochs"], "type": "greater_than_equal", "message": "Input should be greater than or equal to 1"}
    ]
  }
}
```

For `404`, `code` is `job_not_found`, `message` is `Job not found`, and `details` is empty. Validation failures omit raw input and validator context; malformed JSON and invalid path parameters use the same `422` envelope. Generic framework errors such as an unknown route or unexpected server failure are outside this job error contract.

For `503`, `code` is `storage_unavailable`, `message` is `Job storage is unavailable`, and `details` is empty. SQL, connection strings and raw driver errors are not exposed.

## Create job

Implemented contract: `POST /v1/jobs` with optional `Idempotency-Key` and JSON:

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

Validation/create/get (MLP-002), persistence (MLP-003), and durable idempotency/cancellation (MLP-005) are implemented. List/model/health/metrics operations below remain target contracts for later milestones.

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
  "model": "tiny-mlp",
  "dataset": "synthetic-v1",
  "epochs": 5,
  "max_retries": 2,
  "attempt": 1,
  "requested_resources": {"cpus": 2, "gpus": 0, "memory_gib": 2},
  "checkpoint_uri": null,
  "artifact_uri": null,
  "result_model_id": null,
  "result_model_version": null,
  "waiting_reason": null,
  "error": null,
  "created_at": "2026-11-09T10:00:00Z",
  "updated_at": "2026-11-09T10:00:02Z",
  "requested_cancel_at": null,
  "retry_not_before": null
}
```

## Retry and cancellation semantics

`max_retries` is the number of additional controller-managed attempts after the initial run: at most `1 + max_retries` attempt records. Reconciliation and repeat submission of the same attempt do not consume another retry. A confirmed attempt failure may be retried with backoff; an unknown outcome must first be reconciled. Runtime recovery ownership is specified in [reliability](../02-architecture/reliability.md#retry-ownership).

Cancel behavior:

- Unknown or other-tenant job: `404`.
- Safely undispatched job: atomically cancel and return `200` with `CANCELLED`.
- Work may exist remotely or a submission may still arrive: persist cancellation intent and return `202` with `CANCELLING`; repeated requests do not dispatch new work.
- Already terminal job: return `200` with its unchanged state. This is an idempotent API operation, not a terminal-state self-transition.

The cancel response is `JobView`; repeated cancellation preserves the original intent timestamp. `409` uses code `idempotency_conflict`, message `Idempotency key has a different payload`, and empty details. No request key or existing payload is echoed.

See [implemented retry and cancellation policies](../06-engineering/job-policies.md) for failure classification, jitter and the adapter revocation contract. Cancellation can race with success. Guarded DB writes and authoritative attempt checks commit the observed outcome according to the [lifecycle guards](job-lifecycle.md#guards-for-persistent-execution).

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
