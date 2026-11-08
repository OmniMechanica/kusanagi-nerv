# Data model and consistency

## Proposed tables

```text
tenants(id, name, max_active_jobs, gpu_quota)
jobs(id, tenant_id, idempotency_key, payload_hash, spec_json,
     status, state_version, latest_attempt, retry_not_before, created_at, updated_at,
     result_model_id, requested_cancel_at, waiting_reason)
job_attempts(id, job_id, attempt_no, submission_id, ray_cluster_ref, status,
             lease_owner, lease_epoch, lease_expires_at, submission_intent_at,
             started_at, finished_at, reservation_held,
             error_class, error_text, resume_checkpoint_uri, checkpoint_uri)
models(id, tenant_id, source_job_id, version, artifact_uri,
       deploy_state, deployment_identity, deploy_error, created_at)
```

This is an evolving proposal, not a final migration schema. Store secrets outside these rows. Use UTC timestamps and JSON specs only where they do not undermine query/index requirements.

## Critical indexes / constraints

- Unique `(tenant_id, idempotency_key)` (or separate idempotency table with canonical payload hash).
- Unique `(job_id, attempt_no)`; a job is a logical request, an attempt is an execution.
- Unique `submission_id`; persist it before any remote submit. Retrying the network operation reuses this ID and does not increment `attempt_no`.
- At most one unfinished authoritative attempt per logical job; enforce this transactionally, with a partial unique constraint or equivalent guard.
- Unique `models.source_job_id` for the MVP's one model per successful job. Model ID/version/artifact reference are immutable after registration; `jobs.result_model_id` references that record.
- Index `(status, created_at)` for the dispatcher; index `(tenant_id, created_at, id)` for stable pagination.
- Add `state_version` or guarded transitions to protect concurrent updates.

## Dispatch transaction (conceptual)

1. Create the job and queue admission in the same API transaction. A persisted `CREATED` row must not be left waiting for an unspecified enqueue step.
2. Select eligible `QUEUED` jobs in a transaction (`FOR UPDATE SKIP LOCKED` or safe equivalent). Exclude cancellation intent and active attempts; check `retry_not_before` before returning a retry to the queue.
3. Create the attempt, persist `submission_id`, cluster reference and submission intent, and acquire its lease. From MLP-017, reserve tenant capacity atomically too. Commit before the external call. This is the point after which cancellation must assume a remote execution may exist.
4. Submit using that identity and selected resume checkpoint. Leave the job `QUEUED` while the runtime is pending; update it when execution is observed. A fast terminal result can be committed without inventing a running timestamp.
5. Atomically finalize the attempt and guarded job state only if the attempt and lease generation remain authoritative. From MLP-011, training success also verifies the published artifact, creates/reuses the model record and sets `result_model_id` in the same transaction. Earlier dummy-executor milestones do not register models. A stale writer cannot publish a different result or overwrite a terminal outcome.
6. On controller crash or lease expiry, acquire ownership and reconcile the persisted submission identity. `UNKNOWN` retains the attempt. `NOT_FOUND` may permit resubmission of the same identity only under current dispatch authority and with no cancellation intent; it does not by itself prove safe cancellation.
7. After confirmed failure/non-execution, finalize the old attempt and either schedule bounded retry with a durable backoff timestamp or commit terminal failure. Cancellation suppresses retries. Release any tenant reservation only after execution and in-flight dispatch can no longer consume it.

Do not promise mathematically exactly-once side effects in a distributed system. Deliver at-least-once scheduling plus idempotency/reconciliation safeguards.

## Tenant reservations (MLP-017)

An active job is one with a held execution reservation. Reserve one slot and the requested GPU quota before submission; keep it during remote pending, running, unknown outcomes and cancellation. Local queued jobs without an attempt and retry backoff have no reservation. Quota exhaustion leaves the job queued with a visible reason; quotas do not turn waiting into training failure.

Serialize capacity checks and reservation updates per tenant, for example by locking the tenant row in the claim transaction. Locking different job rows with `SKIP LOCKED` is insufficient to enforce a shared tenant cap. Release only after confirmed completion or safe non-execution; a retry must acquire capacity again. Reservations are admission accounting, not measured GPU allocation time or billable usage.

Required race test: two controllers claim different jobs with one tenant slot remaining; exactly one reservation succeeds. API idempotency and attempt uniqueness must also prevent repeated requests or retries from acquiring extra slots.

## Tradeoffs

PostgreSQL is good enough for this capstone and reduces operational complexity. A dedicated broker/event stream becomes attractive when throughput, fan-out, temporal decoupling or failure isolation justify it. Explain a growth migration rather than introducing Kafka on day three.
