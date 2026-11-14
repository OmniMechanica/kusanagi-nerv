# Idempotent admission, bounded retries and cancellation

MLP-005 builds on the [durable controller](controller.md). Apply
`uv run --locked alembic upgrade head` before starting the API/controller; the
current head is `0004_ray_dispatch`. This guide's MLP-005 runtime evidence is
**SIMULATED**. MLP-007 independently tests the [Ray Jobs adapter](ray-jobs.md),
including its stricter missing-job cancellation limits and failure classification.

## Request identity

```bash
curl -i http://127.0.0.1:8000/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: training-request-001' \
  -d '{"model":"tiny-mlp","dataset":"synthetic-v1","epochs":5,"resources":{"cpus":2,"gpus":0,"memory_gib":2},"max_retries":2}'
```

Repeat the same request: `202`, the same job UUID and the same `Location`. Change
the body while retaining the key: `409 idempotency_conflict`. With no key, each
POST creates a new job. Keys are opaque, case-sensitive, tenant-scoped strings of
1–255 visible ASCII characters, with no whitespace; duplicate headers are rejected
with `422`. No key expiry or retention cleanup is implemented.

Canonicalization operates on the validated specification: defaults, sorted keys,
and normalized CPU/memory floating-point values. All fields, including retry
budget and requested resources, participate. Unknown fields remain invalid. The
database's existing `UNIQUE (tenant_id, idempotency_key)` enforces admission across
processes; a process-local cache is insufficient.

The insert uses `ON CONFLICT DO NOTHING RETURNING`. If another transaction wins,
a separate SELECT compares its stored digest and returns its job without updating
state or timestamps. The separate statement matters under PostgreSQL
[Read Committed isolation](https://www.postgresql.org/docs/18/transaction-iso.html#XACT-READ-COMMITTED):
a conflicting insert can wait for a row that was not visible in its original
snapshot. No automatic retry is performed after an uncertain database commit.
The client retries using the same tenant/key and payload to resolve that uncertainty.

The POST response replays the original queued admission acknowledgement, even if
the job has since completed or been cancelled. GET `Location` for current state;
replay does not reactivate a terminal job. A new logical run needs a new key.

## Retry ownership and timing

Executor observations carry a `FailureKind` for confirmed `FAILED`/`REJECTED`:

| Classification | Examples / policy |
|---|---|
| `transient` | Runtime confirms an attempt ended due to a recoverable worker/infrastructure failure; eligible for retry |
| `permanent` | Invalid workload/configuration or unrecoverable execution failure; terminal, also the conservative default |
| `UNKNOWN` observation | Timeout, lost reply or inaccessible runtime; keep the same unfinished attempt, without consuming another retry |

The adapter owns classification from trustworthy terminal evidence. An HTTP/driver
timeout is not a transient **execution** failure. The runtime must also make the
terminal outcome definitive for the submission identity, including delayed submits.

`max_retries = N` allows at most `1 + N` attempts. A retryable failure atomically
finishes its attempt, records the failure kind, moves the job to `RETRYING`, and
stores `retry_not_before` using database time. Equal jitter samples half to all of
the exponential ceiling: 1, 2, 4, 8, … seconds, capped at 60 seconds. The first
delay is therefore 0.5–1 second. The selected timestamp is persisted once; restarts
or competing controllers do not redraw it.

After the deadline, a claim checks the previous confirmed transient failure, budget,
absence of an unfinished attempt, capacity and absence of cancellation intent.
It transitions `RETRYING → QUEUED` and creates a new numbered attempt with new
attempt/submission IDs in one transaction. The logical job ID stays the same.
Exhaustion or permanent failure produces terminal `FAILED`; no terminal job is
eligible for automatic retry. A retry releases the controller's local slot while
waiting; unknown execution still occupies its slot.

The normal CLI simulator succeeds or is cancelled; transient/permanent failure
paths are injected by integration-test adapters. No checkpoint resume is implied.

## Cancellation intent and confirmation

```bash
curl -i -X POST http://127.0.0.1:8000/v1/jobs/JOB_UUID/cancel
```

The response is the current `JobView`, including `requested_cancel_at` and
`retry_not_before`. The HTTP handler performs no executor calls.

| Situation | Response / behavior |
|---|---|
| Unknown or other tenant's job | `404`, identical error envelope |
| Undispatched queued job, or retry backoff after confirmed termination | `200 CANCELLED`; no new attempt |
| Submission intent exists or execution is running/unknown | `202 CANCELLING`; persist intent and reconcile |
| Already cancelling | `202`, preserve intent timestamp; no new work |
| Already terminal | `200`, unchanged terminal record |

Cancel, claim and observation all lock the job first. The current lease owner can
consume a newer cancellation version under that lock without waiting for takeover;
owner/epoch/attempt/expiry checks still apply. Pending/running/unknown observations
cannot turn `CANCELLING` back into normal execution. The controller checks intent
again after lookup and before submit, but cancellation may still race with an
already-started external call.

The adapter's `cancel(submission_id)` may return `CANCELLED` only when execution is
stopped **and** delayed/concurrent submissions of that identity are durably revoked.
The simulator achieves this with a row that records cancellation even for a missing
identity. Submits cannot overwrite that row; lookup, cancellation and elapsed-time
completion are serialized. These records have no cleanup/expiry yet.

`NOT_FOUND`, a stop request acknowledgement or a timeout alone is insufficient.
An adapter unable to prove revocation must return `UNKNOWN`; the job remains
`CANCELLING`. The [MLP-007 Ray adapter](ray-jobs.md) proves this with DB admission/
revocation plus retained terminal Ray identities; an admitted but missing job
cannot be treated as cancelled.
Lease expiry alone does not revoke remote requests.

Observed success can win the race (`CANCELLING → SUCCEEDED`). Confirmed failure
produces `FAILED`; confirmed stop or definitive pre-execution rejection produces
`CANCELLED`. No retry follows cancellation intent, including a transient failure.
If a runtime reports cancellation without platform intent, the attempt ends and
the job becomes `FAILED`; it is not recorded as a user-requested cancellation.

## Reproduce the evidence

```bash
export KUSANAGI_TEST_DATABASE_URL='postgresql+asyncpg://kusanagi:local-development-only@127.0.0.1:55432/postgres'
uv run --locked pytest -q tests/integration/test_job_policies.py tests/integration/test_api_restart.py
uv run --locked pytest -q
```

Tests use real PostgreSQL transactions with barriers/events. They cover duplicate
POSTs, conflicting payloads, uncertain commit recovery, key persistence across API
process death, bounded attempts, persisted jitter, competing retry claims,
claim/cancel and finish/cancel races, unknown submit/stop outcomes, and a delayed
old-owner submit arriving after cancellation confirmation. A separate test proves
lookup waits for an uncommitted cancellation transaction.

Retry tests make deadlines due explicitly in their isolated database rather than
sleeping through backoff. Simulator outcomes and fault injection are `SIMULATED`;
database coordination and process recovery are `TESTED-CPU`. This simulator suite
does not establish exactly-once execution, production tenant auth, real resource
allocation, Ray, checkpoint recovery or GPU evidence. Downgrading a database containing new
cancellation outcomes/tombstones to MLP-004 is rejected by constraints rather
than silently deleting or rewriting history; normal operation uses forward migrations.
