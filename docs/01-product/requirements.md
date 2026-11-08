# Product Requirements Document (PRD)

## Product user story

As a data scientist, I want to submit a model training task with CPU/GPU requirements, receive an id, track state, cancel work, recover after failures, and promote a successful model to inference **without manually provisioning infrastructure**.

## User-facing contracts

**Create job** — `POST /v1/jobs` with `Idempotency-Key` and body:

```json
{
  "model": "tiny-mlp",
  "dataset": "synthetic-v1",
  "epochs": 5,
  "resources": {"cpus": 2, "gpus": 0, "memory_gib": 2},
  "max_retries": 2
}
```

Return HTTP 202, `job_id` (UUID), `status: "queued"` and Location header. Duplicate tenant/key + same payload => same job; duplicate tenant/key + different payload => 409. Tenant identity comes from trusted auth context, or the explicitly test-only context before MLP-017; `tenant_id` is rejected in the request body. See the canonical [API contract](api-contracts.md).

**Job status** — `GET /v1/jobs/{id}` returns requested specs, current state, attempt count, error/waiting reason, checkpoint reference, timing and successful result model ID/version/artifact URI, subject to tenant authorization.

**Cancellation** — `POST /v1/jobs/{id}/cancel` requests best-effort cancellation; controller reconciles Ray task/driver and confirms final status. Races are expected.

**Inference (P0)** — Read `result_model_id` from the successful job, call `POST /v1/models/{id}/deploy`, poll `GET /v1/models/{id}` until ready, then call `POST /v1/models/{id}/predict`, returning prediction and version. Model weights are loaded from object storage. This public API flow is implemented in MLP-011 and tested in MLP-013.

**SDK (P1)** — `client.submit_training(spec)`, `client.get_job(id)`, `client.cancel_job(id)`, `client.deploy_model(id)`, `client.get_model(id)`, `client.predict(model_id, inputs)`. An HTTP client can demonstrate the full P0 flow without the SDK.

## Non-functional / SLO-oriented goals (illustrative learning targets)

- API acknowledges valid submissions within 300 ms p95 *without* waiting for training.
- Job record and idempotency survive API restarts.
- At-least-once dispatch; duplicate reconciliation does not create uncontrolled duplicate training.
- From MLP-017, at most 2 active execution reservations per test tenant; caps are configurable. Submitted/pending, running and cancelling attempts retain their slot until safe release; local waiting and retry backoff do not consume it.
- Queue wait and training duration have separate measurements.
- Failure injection demonstrates real checkpoint-based resume (not just retry-from-zero).
- Log and metric labels avoid job-id-level Prometheus cardinality explosions; rich IDs live in logs/traces.
- No API or worker holds a long-lived production credential in source control.

## Out of scope for 3 weeks

Real distributed scheduler rewrite, production KMS integration, true multi-region HA, full model registry, opaque user-supplied training containers, exhaustive security hardening, realistic cost billing, full GPU-sharing hardware isolation. Document these as tradeoffs.
