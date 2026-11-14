# Ray Jobs adapter: run, reconcile and cancel

MLP-007 connects the durable controller to a separately running local Ray Jobs
cluster. **TESTED-CPU** means actual driver processes performing deterministic
hashing, with PostgreSQL storing attempt identity and results. The workload does
not train a model, create checkpoints/artifacts or register a model. The explicit
`--executor simulated` option remains available for the earlier exercises.

## Run the local system

Install the locked runtime extra and migrate the development database:

```bash
uv sync --locked --extra ray-jobs
docker compose up -d --wait postgres
export KUSANAGI_DATABASE_URL='postgresql+asyncpg://kusanagi:local-development-only@127.0.0.1:55432/kusanagi'
uv run --locked --extra ray-jobs alembic upgrade head
```

In a dedicated terminal, start the Ray cluster and leave it running:

```bash
uv run --locked --extra ray-jobs python -m kusanagi_nerv.runtime.local \
  --info-file /tmp/kusanagi-ray.json --cpus 2 --port 8265
```

The launcher refuses to overwrite an existing info file. Choose an unused path
if another cluster owns that one. `--port 0` selects an available ephemeral port.
The info file contains the loopback address, Ray `session_name`, owner PID and
path to a private temporary authentication token; it does not contain the token.
The launcher publishes the complete file atomically. Token authentication is
configured before importing Ray, dashboard HTTP binds to loopback, and the
runtime has zero GPUs and a 128 MiB object store. No existing cluster is attached.

In another terminal, export the same database URL and start the API:

```bash
export KUSANAGI_DATABASE_URL='postgresql+asyncpg://kusanagi:local-development-only@127.0.0.1:55432/kusanagi'
uv run --locked --extra ray-jobs uvicorn kusanagi_nerv.api.app:create_app \
  --factory --host 127.0.0.1 --port 8000
```

Then start the controller in its own terminal with the same database URL:

```bash
export KUSANAGI_DATABASE_URL='postgresql+asyncpg://kusanagi:local-development-only@127.0.0.1:55432/kusanagi'
uv run --locked --extra ray-jobs python -m kusanagi_nerv.controller \
  --executor ray --ray-info-file /tmp/kusanagi-ray.json
```

Keep `--extra ray-jobs` on every `uv run` sharing this environment: syncing
without the extra can remove packages used by the running Ray cluster. This
increment requires the cluster and controller on the same host with the same
installed Python environment. HTTP control does not import Ray in the controller;
Ray's optional runtime packages are used by the separately launched cluster.

Submit a job and poll the returned `Location`:

```bash
curl -i http://127.0.0.1:8000/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: ray-dummy-demo-1' \
  -d '{"model":"tiny-mlp","dataset":"synthetic-v1","epochs":8,"resources":{"cpus":1,"gpus":0,"memory_gib":0.25},"max_retries":2}'
```

The API returns `202` after committing admission, without contacting Ray. The
controller submits the fixed dummy entrypoint. Each CPU step performs one million
SHA-256 iterations; steps are capped at `min(epochs, 8)` because this is an
execution-contract exercise. Model/dataset/spec values seed the deterministic
checksum; they do not select executable code. CPU and memory requests reserve
logical Ray resources for the entrypoint. They are not OS usage limits. GPU jobs
remain queued with zero attempts on this CPU-only adapter.

`RUNNING` requires a workload `started` log event tied to this submission ID;
a Ray driver in `RUNNING` without that event is still reported as pending. A
successful Ray exit also requires a valid `completed` record. The controller
stores `worker_pid`, `completed_steps` and `checksum` in
`job_attempts.execution_result` under the normal lease/version guards. Public
model/artifact fields remain null. To inspect evidence:

```sql
SELECT submission_id, executor_target, lease_epoch, last_observation,
       dispatch_started_at, dispatch_revoked_at, started_at, finished_at,
       execution_result
FROM job_attempts
WHERE executor_kind = 'ray-jobs-v1'
ORDER BY claimed_at DESC LIMIT 5;
```

Wait for work to reach a terminal state before shutting down the cluster. Stop
the controller separately; it does not stop Ray or cancel its jobs. Ctrl-C or
SIGTERM on the cluster launcher shuts down that owned runtime and removes its
token, runtime directory and info file. SIGKILL can prevent Python cleanup.

## Submission identity and recovery

The claim transaction persists `kusanagi-<attempt UUID>` before dispatch; the UUID
belongs to one immutable `(job_id, attempt_no)` record. The target is persisted as
`<HTTP origin>#<Ray session_name>`. Recovery filters on both executor kind and
target; it never moves an existing attempt to a different configured cluster.
The adapter also checks `/api/version` against the expected session before
external operations. Token material is never stored in the ledger.

Ray receives that exact `submission_id` on every submission. A duplicate request
raises an error; the adapter looks up the existing identity and verifies its
entrypoint and job/attempt/payload metadata. It does not invent a new ID, adopt a
foreign job, or interpret an HTTP error as a training failure. Ray 2.60.0 reserves
the job-info key with `overwrite=False`; concurrent duplicate submissions are
tested against the real server.

After controller death, the replacement waits for lease expiry and reconciles
the same identity. `UNKNOWN` retains the attempt. Before confirmed acceptance,
`NOT_FOUND` can cause another submission of that same identity, after a fresh
authority check. Once accepted, a missing Ray job stays unknown. A fast success
can finalize from `QUEUED` without inventing a `started_at` timestamp.

## Why cancellation needs two systems

Migration `0004_ray_dispatch` adds `executor_target`, `dispatch_started_at`,
`dispatch_revoked_at` and `execution_result` to attempts. Existing simulated
attempts retain target `local`. Dispatch admission and revocation lock the job
first, then the attempt, consistently with controller/API transactions.

| Evidence | Allowed cancellation outcome |
|---|---|
| Revocation committed and no dispatch ever admitted | `CANCELLED`; a delayed caller cannot pass the DB guard |
| Dispatch admitted, Ray returns `404` or transport is unavailable | `UNKNOWN`; job remains `CANCELLING` |
| Ray accepts `stop` but still reports pending/running | Keep reconciling; stop acknowledgement is not termination |
| Matching Ray job reports `STOPPED` | `CANCELLED`, relying on the retained Ray identity to reject late duplicate requests |
| Matching job completed first | Preserve observed success or failure |

`dispatch_started_at` commits **before** sending HTTP. It means a request may be
in flight, not that Ray accepted it. Once cancellation revokes dispatch, no new
caller can pass the DB guard. A caller already past that guard can still arrive;
cancellation waits for its Ray identity to become observable and terminal. The
integration test pauses precisely there, then sends a truly late duplicate HTTP
request after terminal cancellation and verifies rejection by Ray.

If a process dies after DB dispatch admission but before sending anything, then
cancellation may remain unknown indefinitely. A missing remote record cannot
distinguish that case from a delayed request. This favors safety over automatic
completion; no timeout is used to fabricate cancellation evidence.

Ray job records must remain retained for these identity guarantees. Do not delete
them while stale requests can arrive. Ray head loss/recreation and manual metadata
deletion are outside this recovery claim. Session checks reject an observed
cluster change; they are not an atomic fence against a head replacement occurring
between an HTTP identity check and a submit.

## Failures and limits

- Connection errors, timeouts, malformed replies, identity mismatch and missing
  completion evidence become unknown outcomes. The controller does not consume
  the attempt retry budget for transport uncertainty.
- A confirmed Ray `FAILED` is currently classified as permanent. A generic driver
  failure does not establish retryability, even with `max_retries > 0`. Typed
  transient training errors/checkpoint recovery are later work; the existing
  controller budget remains available to adapters that can establish that evidence.
- The dummy uses a trusted fixed command and two small structured log events.
  The Ray Jobs logs endpoint is the current start/result channel. Log removal
  before result capture can leave success unresolved; production event/artifact
  transport and log lifecycle are later work.
- One active attempt per controller, fixed test tenant, no production auth,
  fairness or quota scheduler. Loopback HTTP and token authentication are local
  development controls, not production tenant isolation.
- Downgrade refuses to discard Ray attempt history/dispatch fences. Upgrades
  preserve existing jobs and simulated attempts. Do not downgrade a working
  development ledger as a cleanup procedure.

## Reproduce verification

```bash
export KUSANAGI_TEST_DATABASE_URL='postgresql+asyncpg://kusanagi:local-development-only@127.0.0.1:55432/postgres'
uv run --locked --extra ray-jobs pytest -q tests/integration -m ray_jobs
uv run --locked --extra ray-jobs pytest -q tests/integration -m 'not ray_jobs'
uv run --locked --extra ray-jobs pytest -q
```

The Ray Jobs suite starts and stops its own local cluster and creates/drops only
owned disposable databases. `KUSANAGI_TEST_RAY_INFO` can explicitly select an
already-running test cluster; the test then leaves that cluster running. Missing
database configuration or optional packages produces skips, not runtime evidence.

Five real Ray Jobs cases cover API admission/process separation, actual controller
death/recovery, crash after submit/before ledger acknowledgement, concurrent
duplicate submits, completion between polls, delayed submit/cancel and confirmed
driver failure. Separate PostgreSQL tests cover dispatch/cancel transaction races,
target pinning, stale leases, unknown lookup and migration protection. Transport
unit tests exercise malformed/foreign responses and missing start/result signals.

Code: [adapter](../../src/kusanagi_nerv/executors/ray_jobs.py),
[dispatch guards](../../src/kusanagi_nerv/repositories/attempts.py),
[dummy workload](../../src/kusanagi_nerv/workloads/dummy.py),
[cluster launcher](../../src/kusanagi_nerv/runtime/local.py),
[real integration tests](../../tests/integration/test_ray_jobs.py).

Learning checkpoint: explain which ID survives a controller crash, why a lost
submit reply cannot create a new attempt, why `404` cannot confirm cancellation
after dispatch admission, and what is lost if the Ray head is recreated.

Primary references: [Ray Jobs REST API](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/rest.html),
[submit_job identity and resources](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/doc/ray.job_submission.JobSubmissionClient.submit_job.html),
[Ray 2.60.0 job manager](https://github.com/ray-project/ray/blob/ray-2.60.0/python/ray/dashboard/modules/job/job_manager.py).
