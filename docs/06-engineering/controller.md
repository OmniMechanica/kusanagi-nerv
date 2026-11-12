# Durable controller: run, recover and inspect

MLP-004 introduces a separate controller process. The API still only commits and
reads logical jobs. Start PostgreSQL and apply `alembic upgrade head` using the
[database guide](postgres.md); the current head is `0003_retry_cancel`. Neither
the API nor the controller creates tables at startup.

## Run the simulation

With the same database URL as the API, in a separate terminal:

```bash
export KUSANAGI_DATABASE_URL='postgresql+asyncpg://kusanagi:local-development-only@127.0.0.1:55432/kusanagi'
uv run --locked python -m kusanagi_nerv.controller --executor simulated
```

Submit a CPU job using the [README example](../../README.md#run-the-local-job-api).
GET its `Location` to see `attempt: 1`, usually `running`, then `succeeded` after
about three seconds. A sufficiently fast execution can transition directly from
queued to success; a missed running observation does not invent `started_at`.

**SIMULATED:** the executor records acceptance and a completion timestamp in
`simulated_executions`; elapsed database time determines completion. It performs
no ML work, consumes no allocated GPU/CPU training resources and produces no
model or artifact. `result_model_id` remains null. This durable fake survives
controller restarts, but shares PostgreSQL with the ledger: it does not reproduce
an independent runtime's availability or network topology.

Start a second copy with the same command to exercise multiple owners. Each
process generates its own owner ID and handles at most one active attempt.
Stop a controller with Ctrl-C/SIGTERM; its attempt remains recoverable. Process
shutdown is not job cancellation. A replacement can claim it after lease expiry.

Timing flags, in seconds:

| Flag | Default | Meaning |
|---|---:|---|
| `--lease-seconds` | 30 | DB-authoritative ownership TTL, at most 3600 |
| `--poll-seconds` | 1 | Delay between bounded reconciliation steps |
| `--executor-timeout-seconds` | 5 | Deadline per lookup or submit |
| `--simulation-seconds` | 3 | Persisted fake runtime duration, 0–3600 |

The lease must exceed three times each of the poll interval and executor timeout;
values must be finite, and all but simulation duration must be positive. Database
timeouts are independently bounded as described in the storage guide. Scheduling
delays or slow DB calls can still expire a lease; guards reject its subsequent
writes. Increasing TTL reduces takeovers but delays crash recovery.

## Ownership and external calls

1. Lock an eligible job with `FOR UPDATE SKIP LOCKED`. Prefer expired unfinished
   attempts for this executor; otherwise admit an undispatched queued CPU job or a due, eligible retry without cancellation intent.
2. For a new attempt, persist its UUID, stable `submission_id`, executor kind,
   submission intent and owner/epoch/expiry in the same transaction that increments
   the job attempt count and version. Dispatch starts only after confirmed commit.
3. Renew the current lease and look up the saved identity outside the transaction.
   Only `NOT_FOUND` before observed acceptance allows submitting that same identity,
   after another authority check. The adapter must make duplicate submit idempotent.
4. Persist observations only under the current attempt, job version, owner, epoch
   and unexpired lease. A newer cancellation version is consumed under the job lock without weakening attempt/lease checks. Job state and terminal attempt metadata commit together.
5. After expiry, another controller increments the epoch and keeps the attempt and
   submission IDs. It reconciles first; it does not allocate a replacement attempt.

Every attempt operation locks the job before updating the attempt, providing a
consistent lock order. A partial unique index also forbids two unfinished attempts
for one job. A generic version-only `update_status()` is restricted to jobs with
`attempt = 0`; it cannot bypass execution guards after dispatch intent exists.

Lease comparisons use PostgreSQL `clock_timestamp()` after the job lock is acquired.
Transaction-start `now()` would be stale after a lock wait. PostgreSQL documents
[wall-clock versus transaction time](https://www.postgresql.org/docs/18/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT)
and the queue-consumer use of [SKIP LOCKED](https://www.postgresql.org/docs/18/sql-select.html#SQL-FOR-UPDATE-SHARE).
The authority check is the write's linearization point; row locks prevent a
takeover from interleaving before commit.

An epoch fences ledger writes, not external side effects. An old request may
already be in flight when ownership changes. Safety therefore also depends on
durable runtime identity/idempotency and reconciliation. This is not an
exactly-once execution claim. Ray's adapter must establish these semantics in MLP-007.

## Outcome handling

| Observation | Current behavior |
|---|---|
| `PENDING` | Same unfinished attempt, queued job, `executor_pending` reason |
| `RUNNING` | Record observed start and advance queued job |
| `SUCCEEDED` | Atomically finish attempt and job; no model registration yet |
| `FAILED` | Confirmed termination; retry only for a transient failure with budget and no cancel intent |
| `REJECTED` | Confirmed non-execution; same retry policy, or cancelled if cancellation was requested |
| `CANCELLED` | Confirmed stop plus late-submit revocation; complete requested cancellation |
| Timeout / `UNKNOWN` | Keep attempt open and `executor_outcome_unknown`; reconcile later |
| `NOT_FOUND` after observed acceptance | Keep outcome unknown; no resubmission or failure |

Previously observed acceptance is sticky. A contradictory later rejection is
treated as unknown. Pending observations do not regress a running job to queued.
Raw adapter messages are not placed in job errors; confirmed failures expose
`executor_failed` or `executor_rejected`.

Storage errors drop the local claim without dispatching further work. Even if a
commit succeeded but its response was lost, recovery uses the saved identity after
expiry. Unexpected programming errors stop the process; supervisor restart and
lease recovery are preferable to silently classifying them as training failure.

## Inspect attempts

Logs include owner, job ID, attempt ID, submission ID, epoch and observation. With
the local Compose database:

```bash
docker compose exec postgres psql -U kusanagi -d kusanagi -c \
  'SELECT job_id, id, attempt_no, submission_id, executor_kind, lease_owner, lease_epoch, lease_expires_at, last_observation, finished_at FROM job_attempts ORDER BY submission_intent_at;'
```

Use `docker --context desktop-linux compose ...` if needed on macOS, without
changing the global Docker context. Attempt inspection is currently internal SQL;
the public job API exposes the count and job state, not lease details.

## Verification and limits

```bash
export KUSANAGI_TEST_DATABASE_URL='postgresql+asyncpg://kusanagi:local-development-only@127.0.0.1:55432/postgres'
uv run --locked pytest -q tests/integration/test_controller.py tests/integration/test_api_restart.py
uv run --locked pytest -q
```

`TESTED-CPU` covers real PostgreSQL transactions, a barrier with two active database
transactions, locked-row skipping, stale owner/epoch/expiry rejection, expiry while
waiting for a lock, migration preservation and real API/controller process death.
The controller restart test kills an actual process, waits for natural lease expiry
and verifies the replacement preserves the execution identity. Other failure tests
expire a lease explicitly or inject transport outcomes to avoid timing races.
Executor behavior in all these tests is **SIMULATED**.

MLP-005 implements [HTTP idempotency, bounded retries/backoff and cancellation](job-policies.md).
The adapter returns typed observations and transient/permanent failure classification;
transport uncertainty never consumes another attempt. GPU requests
remain queued with zero attempts; GPU capacity reasons/scheduling arrive later.
CPU/memory allocations, tenant quotas, scheduling fairness and production auth are
not implemented. A permanently unknown attempt occupies its controller's single
slot; this minimal polling model has no fairness/throughput claim. No Ray, training,
checkpoint recovery, GPU performance or model deployment has been validated.
