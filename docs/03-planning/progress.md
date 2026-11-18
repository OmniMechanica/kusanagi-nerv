# Progress tracker and evidence journal

**Last updated:** 2026-11-18\
**Status:** MLP-001–010 are integrated into main. MLP-011, MLP-013 and MLP-015–016 are in local review. Learning checkpoints remain pending.

## Checkpoint gates

- [ ] **Gate A / Day 5:** typed API + durable ledger + controller + idempotency/cancellation tests
- [ ] **Gate B / Day 13–14:** CPU end-to-end training → checkpoint restore → inference; Python + architecture mock
- [ ] **Gate C / target Day 19 (optional full extension):** KubeRay/GPU design, tested tenant policy, metrics/cost, expanded CI and ADRs
- [ ] **Gate D / target Day 21:** CPU chaos evidence, demo, coding mock, KubeRay/GPU design (`MLP-015–016`), system design and STAR stories

Gate D can proceed without full Gate C. Record deferred P1 work explicitly and leave Gate C unchecked until all its criteria pass. SDK, live tenant controls and advanced metrics are optional demo extensions; design discussions do not prove implementation.

## Daily tracking

| Day | Ticket | Status | Evidence link / command | Blocker / next action |
|---:|---|---|---|---|
| 01 | MLP-001 | In review | [Lifecycle evidence](#2026-11-09--mlp-001--job-lifecycle-implementation), [merged PR #4](https://github.com/OmniMechanica/kusanagi-nerv/pull/4) | Technical review approved; finish learning checkpoint |
| 02 | MLP-002 | In review | [API evidence](#2026-11-09--mlp-002--typed-job-api) | PR #5 merged into main; rehearse 202/async/durable-queue tradeoffs |
| 03 | MLP-003 | In review | [PostgreSQL evidence](#2026-11-11--mlp-003--durable-postgresql-ledger) | Integrated into main via PR #7; complete learning drill |
| 04 | MLP-004 | In review | [Controller evidence](#2026-11-11--mlp-004--durable-controller-and-fenced-leases) | PR #7 merged; complete claim/recovery learning drill |
| 05 | MLP-005 | In review | [Policy evidence](#2026-11-12--mlp-005--idempotency-retries-and-cancellation) | Review concurrent outcomes and complete learning drill |
| 06 | MLP-006 | Planned | — | — |
| 07 | MLP-007 | Planned | — | — |
| 08 | MLP-008 | Planned | — | — |
| 09 | MLP-009 | Planned | — | — |
| 10 | MLP-010 | In review | [Merged PR #13](https://github.com/OmniMechanica/kusanagi-nerv/pull/13) | Complete the checkpoint learning drill |
| 11 | MLP-011 | In review | [Model serving evidence](#2026-11-18--mlp-011--model-serving) | Review the increment and complete the learning drill |
| 12 | MLP-012 | Planned | — | — |
| 13 | MLP-013 | In review | [Public CPU pipeline](#2026-11-18--mlp-013--public-cpu-pipeline) | Review the E2E cases and complete the architecture drill |
| 14 | MLP-014 | In progress | [Coding and system design exercises](tasks/MLP-014.md), [CPU demo evidence](#2026-11-18--mlp-013--public-cpu-pipeline) | First coding pass and scored mock pending |
| 15 | MLP-015 | In review | [CPU batch and serving on kind](#2026-11-18--mlp-015--cpu-batch-and-serving-on-kind) | Review the manifests and explain the failure domains |
| 16 | MLP-016 | In review | [GPU resource contract](#2026-11-18--mlp-016--gpu-resource-contract) | Review capacity and topology tradeoffs |
| 17 | MLP-017 | Planned | — | — |
| 18 | MLP-018 | Planned | — | — |
| 19 | MLP-019 | Planned | — | — |
| 20 | MLP-020 | Planned | — | — |
| 21 | MLP-021 | Planned | — | — |

## Evidence entry template

```markdown
### YYYY-MM-DD — MLP-XXX — Short title
- Status: In review / Done / Blocked
- Evidence: TESTED-CPU / TESTED-GPU / SIMULATED / DESIGN-ONLY
- Code / PR: ...
- Verification command: ...
- Observed result: ...
- Decision/tradeoff: ...
- What I can explain in 3 minutes: ...
- Next step: ...
```

## 2026-11-08 — MLP-001 — Repository bootstrap

- Status: In progress; preparation complete locally, lifecycle exercise pending.
- Evidence: `TESTED-CPU` for environment/package checks on macOS arm64 only.
- Environment: CPython 3.12.16, uv 0.13.7, pytest 9.2.0, Ruff 0.17.4; interpreter pinned in `.python-version`, dependencies in `uv.lock`.
- Code: [package](../../src/kusanagi_nerv/), [installation smoke test](../../tests/smoke/test_package_installation.py), [CI workflow](../../.github/workflows/ci.yml).
- `uv sync --locked`: succeeded; editable package and development dependencies installed.
- `uv run --locked ruff check src tests`: all checks passed.
- `uv run --locked ruff format --check src tests`: all five Python files already formatted.
- `uv run --locked pytest -q`: 1 passed; verifies an isolated interpreter can import the installed package from outside the checkout.
- `uv build`: wheel and source distribution built successfully.
- CI: workflow added for pull requests/pushes to `main`; hosted execution results are tracked in the bootstrap PR checks. Local results do not establish Linux CI success.
- Limits: no lifecycle behavior or domain tests implemented; no Ray, API, DB, GPU or platform integration validated. MLP-001 is not Done.
- Next step: developer implements `JobStatus` and `can_transition()` with independent expected transition cases, then requests review.

## 2026-11-09 — MLP-001 — Job lifecycle implementation

- Status: In review; implementation and local verification complete, learner explanation pending.
- Evidence: `TESTED-CPU` for pure Python domain and package checks on macOS arm64.
- Work mode: assistant implementation with step-by-step explanations, explicitly requested by the user; this is not evidence of independent learner implementation or a completed interview drill.
- Environment: existing CPython 3.12.16, uv 0.13.7, pytest 9.2.0 and Ruff 0.17.4; no dependency changes.
- Code: [lifecycle](../../src/kusanagi_nerv/domain/job_lifecycle.py), [domain tests](../../tests/unit/test_job_lifecycle.py); rationale in [design notes](../01-product/job-lifecycle.md#mlp-001-implementation-notes).
- `uv run --locked ruff check src tests`: all checks passed.
- `uv run --locked ruff format --check src tests`: all five Python files already formatted.
- `uv run --locked pytest -q`: 79 passed, with no warnings (78 domain cases and one package-installation smoke test).
- Test coverage: all 64 state pairs (18 allowed, 46 forbidden), enum names/values, six lifecycle paths and seven invalid-input pairs checked against both public functions. Expected edges are separate from the implementation map.
- `uv build`: wheel and source distribution built successfully.
- Independent review: behavior also compared directly with the specification table; no missing/extra transitions or blocking correctness findings.
- CI: hosted results are tracked in the implementation PR checks; local results alone do not establish Linux success.
- Decision/tradeoff: an immutable graph and two pure functions keep domain rules independent of adapters. Strict enum inputs preserve the declared return type. Retry policy and attempt ownership need execution context and remain outside this module.
- Limits: scenario paths do not prove cancellation races, transaction isolation or remote execution safety. No API, DB, Ray, GPU or model training has been exercised.
- Next step: review the implementation and explain logical job versus attempt, terminal `FAILED`, and the evidence needed to finalize cancellation. Record that learning check before marking MLP-001 Done.

## 2026-11-09 — MLP-001 — Technical review follow-up

- [PR #4](https://github.com/OmniMechanica/kusanagi-nerv/pull/4) is merged as `4729b04`; technical review approved by the user in the conversation. No formal GitHub approval submission is claimed.
- The user authorized moving to MLP-002. The remaining learning checkpoint is still tracked; the technical approval is not evidence that the learner completed it.
- P0 follow-ups already belong to MLP-004/005: cancellation needs execution/dispatch evidence, and concurrent controllers need conditional database writes or optimistic version checks.
- P1 follow-up: consider mypy/Pyright in MLP-019. It does not block MLP-002; no static type checker is currently configured or claimed to have passed.

## 2026-11-09 — MLP-002 — Typed job API

- Status: In review; implementation and local verification complete, review/learning drill pending.
- Evidence: `TESTED-CPU` on macOS arm64; HTTP admission and inspection only, not model training.
- Environment: CPython 3.12.16, uv 0.13.7, FastAPI 0.162.2, Pydantic 2.14.3, Starlette 1.10.0, Uvicorn 0.54.0, HTTPX2 2.16.0; exact dependencies in `uv.lock`.
- Code: [API](../../src/kusanagi_nerv/api/app.py), [schemas](../../src/kusanagi_nerv/api/schemas.py), [domain records](../../src/kusanagi_nerv/domain/jobs.py), [repository](../../src/kusanagi_nerv/repositories/jobs.py), [API tests](../../tests/api/test_jobs.py).
- `uv run --locked ruff check src tests`: all checks passed.
- `uv run --locked ruff format --check src tests`: all 12 Python files already formatted.
- `uv run --locked pytest -q`: 146 passed with no warnings (67 API cases plus the previous 79 lifecycle/package cases).
- `uv build`: wheel and source distribution built successfully.
- Real HTTP smoke: started a temporary Uvicorn factory on a loopback-only ephemeral port; observed POST `202`, GET `200` with queued state/zero attempts, unknown UUID `404`, and successful `/openapi.json` and `/docs` responses. The temporary server was stopped after verification. Reproduce with the [README commands](../../README.md#run-the-local-job-api).
- CI: the existing workflow runs the full test suite, including API tests; hosted results are tracked in the implementation PR checks.
- Verified: resource/epoch/retry bounds, strict input types, required/extra fields, malformed JSON, non-finite resources, tenant-scoped lookup, injected repository substitution, admission failure, per-app storage isolation, and documented response schemas/status codes.
- Decision/tradeoff: Pydantic validates at the HTTP boundary, immutable standard-library records keep the domain independent, and an asynchronous repository protocol leaves room for PostgreSQL. API handlers await admission and do not invoke an executor or `BackgroundTasks`.
- Limits: one fixed local test tenant, no authentication, no durable storage or multi-worker sharing. Each POST creates a new job even with an identical idempotency key. No cancellation, retry execution, Ray, GPU or training run is claimed. No latency SLO or static type-check result is claimed.
- Next step: review the API and explain HTTP 202, async I/O versus CPU work, and why `BackgroundTasks` cannot replace a durable queue; then proceed to MLP-003 persistence.

## 2026-11-11 — MLP-003 — Durable PostgreSQL ledger

- Status: In review; implementation and local verification complete, review/learning drill pending. Based on MLP-002's still-open [PR #5](https://github.com/OmniMechanica/kusanagi-nerv/pull/5); no approval or merge of that PR is inferred from proceeding.
- Evidence: `TESTED-CPU` on macOS arm64 with real PostgreSQL in Docker Desktop; no SQLite substitute or model training involved.
- Environment: CPython 3.12.16, uv 0.13.7, SQLAlchemy 2.1.8, asyncpg 0.32.0, Alembic 1.20.1, pytest-asyncio 1.4.1.post1; pinned PostgreSQL 18.6 Alpine image and digest in Compose/CI. Existing API dependencies remain locked.
- Code: [PostgreSQL repository](../../src/kusanagi_nerv/repositories/postgres.py), [schema](../../src/kusanagi_nerv/db/models.py), [initial migration](../../migrations/versions/0001_job_ledger.py), [DB tests](../../tests/integration/test_postgres.py), [process-crash test](../../tests/integration/test_api_restart.py).
- `docker --context desktop-linux compose up -d --wait postgres`: project-owned local service became healthy. No global Docker context switch was needed.
- With the development URL exported, `uv run --locked alembic upgrade head` succeeded; `alembic current` returned `0001_job_ledger (head)`; `alembic check` reported no new upgrade operations.
- With the test URL exported, `uv run --locked pytest -q`: 174 passed with no warnings, including 18 PostgreSQL integration cases and 156 fast cases.
- `uv run --locked ruff check src tests migrations` passed; formatting check reported all 22 Python files formatted; `uv build` produced the wheel and source distribution.
- Integration evidence: commit/read round trip, metadata/hash storage, rollback and recovery after a rejected write, tenant/identity/status predicates, schema constraints, repeated upgrade without data loss, downgrade/upgrade only in an owned disposable DB, and driver/schema failures exposed as sanitized `503` responses.
- Race evidence: two independently scoped sessions read the same version and synchronize on a barrier; exactly one conditional update commits. A stale version also fails after a retry cycle returns the job to its original status.
- Restart evidence: a real Uvicorn process accepted a job, was killed, and a fresh process returned the identical job view from PostgreSQL.
- Decision/tradeoff: PostgreSQL owns durable queued state. New per-operation sessions commit before acknowledgement; migrations are explicit and startup performs no DDL. The internal compare-and-swap primitive protects versions but does not yet establish attempt/lease authority or remote cancellation safety.
- Limits: fixed test tenant, no production authentication, no idempotency/deduplication or automatic write retries. A lost response during commit may leave the outcome unknown. Attempts/leases and dispatch require MLP-004's next migration; retry/cancellation policy follows in MLP-005.
- CI: fast lint/tests/package and PostgreSQL integration run as separate jobs; all PR targets are supported so dependent feature branches receive checks. Hosted results are tracked in the implementation PR.
- Cleanup: zero disposable `kusanagi_test_*` databases remained; the project PostgreSQL container was stopped after verification. Its named volume retains the migrated development schema. No API test processes remain running.
- Next step: review MLP-002/003 in dependency order and explain commit-before-202, version conflicts and uncertain outcomes before marking the learning tasks Done.

## 2026-11-11 — MLP-004 — Durable controller and fenced leases

- Status: In review; implementation and local verification complete. Learning drill pending; no independent learner implementation is claimed.
- Dependency integration: PR #5 merged into main as `b5b12c9`. PR #6 merged into `feat/mlp-002-job-api` as `cdac562`, not main. The MLP-004 branch starts from main and merges that already-reviewed history; its PR targets main directly.
- Evidence: `TESTED-CPU` for PostgreSQL concurrency, migrations and real process recovery on macOS arm64. Executor results are `SIMULATED`, with no training, model registration or GPU work.
- Code: [controller](../../src/kusanagi_nerv/controller/service.py), [attempt store](../../src/kusanagi_nerv/repositories/attempts.py), [runtime contract](../../src/kusanagi_nerv/domain/execution.py), [simulation](../../src/kusanagi_nerv/executors/simulated.py), [migration](../../migrations/versions/0002_attempt_leases.py), [race tests](../../tests/integration/test_controller.py), [process tests](../../tests/integration/test_api_restart.py).
- Environment: unchanged locked Python/dependency versions and pinned PostgreSQL 18.6 image from MLP-003; no new runtime dependency.
- With the local test database URL exported, `.venv/bin/pytest -q`: **204 passed in 27.88s**, comprising 165 fast cases and 39 real PostgreSQL integration cases.
- Final checks: Ruff lint passed, all 33 Python files passed formatting, 165 fast tests passed after the OpenAPI description update, and `uv build` produced wheel/source distributions. The wheel contains the controller/adapters; the installed module's `--help` works in isolated interpreter mode. Local documentation file links and whitespace checks passed.
- Development schema: explicit `alembic upgrade head` succeeded, `alembic current` returned `0002_attempt_leases (head)`, and `alembic check` found no new operations. Existing development data/volume were retained.
- Cleanup: zero disposable test databases and no API/controller test processes remained. The project PostgreSQL service was stopped; its named volume and migrated schema were preserved.
- Claim evidence: two independent database transactions synchronize on a barrier before claiming one job; exactly one attempt is created. A locked job is skipped while another job remains claimable. A partial unique index rejects another unfinished attempt.
- Fencing evidence: stale owner, stale epoch, expired lease (even without takeover), and terminal-attempt writes are rejected. Another test starts a writer while valid, blocks it on the job lock, expires its lease, then verifies rejection after the lock is released. Version-only cancellation cannot bypass an existing attempt.
- Recovery evidence: API returns 202 with no controller; a real controller process starts the simulation and is killed. A second process waits for natural lease expiry and commits success using the same attempt/submission IDs and one simulated execution. No test-only lease mutation is used in this process-restart scenario.
- Failure evidence: unknown lookup does not dispatch; transport calls have deadlines; lost submit responses before/after acceptance preserve identity; observed accepted work that disappears is not resubmitted or failed; confirmed rejection/termination finalizes once; a stale caller cannot commit success after an external call.
- Migration evidence: schema/model agreement, repeated upgrade, populated MLP-003 ledger preserved through upgrade, and downgrade/upgrade only in disposable databases are covered by the suite.
- Decision/tradeoff: DB-authoritative wall-clock leases plus consistent job-first locking fence ledger writes. Durable adapter idempotency is still required for in-flight external calls; no exactly-once guarantee. The fake runtime shares PostgreSQL for deterministic restart evidence, not for production execution isolation.
- Limits: one active attempt per controller; unresolved unknown work occupies that slot. No fairness, production auth/quotas, actual CPU/memory allocation, Ray, training or artifacts. GPU requests remain queued with zero attempts. Retry budgets, backoff, cancellation and HTTP idempotency remain MLP-005.
- CI: the existing lint/tests/package and PostgreSQL integration jobs cover the new files/tests. Hosted outcomes are recorded in the implementation PR checks, separately from local evidence.
- Next step: review MLP-004 and explain commit-before-dispatch, lease expiry versus worker termination, and why takeover preserves submission identity; then start MLP-005.

## 2026-11-12 — MLP-005 — Idempotency, retries and cancellation

- Status: In review; implementation and local verification complete, learner explanation pending. PR #7 merged MLP-004 and the preceding ledger into main as `8a13b3c`; this increment starts from that main commit.
- Evidence: `TESTED-CPU` for real PostgreSQL transactions, migration and API process restart on macOS arm64. Runtime outcomes and injected failures are `SIMULATED`; no training, Ray, artifact or GPU execution is claimed.
- Code: [admission/cancel storage](../../src/kusanagi_nerv/repositories/postgres.py), [attempt policies](../../src/kusanagi_nerv/repositories/attempts.py), [retry timing](../../src/kusanagi_nerv/domain/retries.py), [runtime observations](../../src/kusanagi_nerv/domain/execution.py), [simulation](../../src/kusanagi_nerv/executors/simulated.py), [migration](../../migrations/versions/0003_retry_cancel.py), [policy tests](../../tests/integration/test_job_policies.py).
- Environment: same locked Python/dependency versions and PostgreSQL 18.6 image as MLP-004; no dependency additions.
- With the local test URL exported, `.venv/bin/pytest -q --tb=short`: **244 passed in 37.32s** — 187 fast cases and 57 PostgreSQL integration cases.
- Idempotency evidence: concurrent HTTP requests enter separate DB transactions via a barrier; equal payloads resolve to one job, different payloads yield one `202` and one `409`. The database unique constraint is inspected. Canonical defaults/numeric representations, tenant-scoped keys and a lost response after committed admission are covered. A real API process is killed and restarted before replaying the same key/Location.
- Retry evidence: zero and two additional-attempt budgets terminate at their exact limits; each retry has a distinct attempt/submission identity and preserves the job ID. Selected jitter deadlines persist across new repository instances. Competing controllers create one due retry; stale observations cannot reschedule it. A controller-level transient failure then success path is covered.
- Cancellation evidence: claim/cancel (initial and retry), final-observation/cancel and runtime-submit/cancel races use barriers; running/unknown accepted work stays `CANCELLING` until confirmation. Unknown/not-found stop outcomes do not consume retry budget or finalize cancellation. Cancellation during backoff or before transient failure suppresses retries. Already-terminal requests return unchanged state.
- Late-submit evidence: an event holds the old owner's submit, cancellation is requested, its lease expires, and a replacement creates a durable runtime revocation record and confirms cancellation. Releasing the old submit returns cancelled without runtime acceptance; its stale ledger write is rejected. Another test verifies lookup waits for an uncommitted cancellation transaction, preserving consistent time-derived completion/stop outcomes.
- Review correction: serialized simulator lookup with cancellation after identifying a possible read of pre-cancellation state while stop was committing. Added a targeted real-DB regression test; the final full suite includes that check.
- Migration/build: explicit local upgrade succeeded; `alembic current` returned `0003_retry_cancel (head)` and `alembic check` found no drift. `uv build` produced wheel and source distribution. Integration tests cover repeated upgrade, existing job preservation and downgrade/upgrade only in owned disposable DBs.
- Final verification/cleanup: Ruff lint and all 38 Python-file formatting checks passed; local documentation file links and whitespace checks passed, and the wheel includes the new policy modules. Zero disposable test databases and no API/controller test processes remained. The project PostgreSQL service was stopped with its named volume and migrated data retained.
- Decision/tradeoff: optional request keys preserve unkeyed admission; keys and runtime revocation records currently have no expiry. Only confirmed transient execution failures can consume new attempts. Cancellation versions refresh the current lease holder under the job lock while attempt/owner/epoch/expiry guards remain mandatory.
- Limits: fixed test tenant; one active attempt per controller; no fairness or real CPU/memory allocation. Unknown runtime outcomes may remain unresolved and occupy that slot. Ray must independently prove identity/revocation semantics in MLP-007; checkpoint resume and training remain later work. Static type checking remains the agreed P1 follow-up. Gate A and ticket Done status still require learner checkpoints.
- CI: existing lint/tests/package and PostgreSQL jobs cover this increment; hosted outcomes are recorded in its PR checks, separately from local evidence.
- Next step: review MLP-005 and explain request identity versus attempt identity, retryable execution failure versus transport uncertainty, and why a missing runtime job cannot alone confirm cancellation. Then proceed to MLP-006's isolated Ray Core tasks/actors exercise.

## 2026-11-13 — MLP-006 — Isolated Ray Core tasks, actors and backpressure

- Status: In review; implementation and local verification complete, learner explanation pending. [PR #8](https://github.com/OmniMechanica/kusanagi-nerv/pull/8) merged MLP-005 into main as `4632ab0`; this branch starts from that main commit.
- Evidence: **TESTED-CPU** for actual Ray tasks/actor in separate processes on macOS arm64. The controller adapter remains **SIMULATED**; no Ray Jobs dispatch, ML training, checkpoint, artifact or GPU execution is claimed.
- Environment: CPython 3.12.16, uv 0.13.7 and Ray 2.60.0 in the new optional `ray` extra. Existing backend dependency versions are unchanged; exact resolution is in `uv.lock`.
- Code: [pure model](../../src/kusanagi_nerv/examples/ray_core/model.py), [tasks/actor](../../src/kusanagi_nerv/examples/ray_core/workers.py), [bounded driver](../../src/kusanagi_nerv/examples/ray_core/runner.py), [real Ray tests](../../tests/ray/test_ray_core.py), [run and learning guide](../06-engineering/ray-core.md).
- `.venv/bin/pytest -q -m 'not integration and not ray'`: **214 passed in 1.37s**. Input validation, concurrency measurement, private runtime environment, existing-driver rejection, package CLI help and API import without Ray are covered.
- `.venv/bin/pytest -q tests/ray`: **5 passed in 25.91s**. Real runtime cases cover CPU/window combinations `(2, 4)`, `(1, 3)`, `(2, 1)`, separate worker/actor/driver PIDs, shared input, state retained across actor calls, task exception, deadline, shutdown and temporary directory removal. A missing optional dependency produces skips, not Ray evidence.
- With `KUSANAGI_TEST_DATABASE_URL` exported, `.venv/bin/pytest -q tests/integration`: **57 passed in 38.99s** against the project's real PostgreSQL 18.6 container. Across the three suites: **276 passing tests**.
- Observed sample: `python -m kusanagi_nerv.examples.ray_core --tasks 8 --workers 2 --max-outstanding 4 --work-iterations 1000000` completed all eight tasks; actor count `0 → 8`, value sum `682780`, `max_running=2`, `peak_outstanding=4`, `backpressure_waits=4`. Driver PID `79095`, actor PID `79126`, task worker PIDs `79125/79127`; CPU capacity `3`, GPU `0`, runtime shutdown confirmed. PIDs are per-run evidence, not fixed expectations.
- Design: the actor reserves one additional logical CPU before task submission. Tasks request one CPU and no GPUs; actor methods are serial. `ray.wait` bounds submitted-but-unconsumed tasks while `ray.put` supplies shared input. Ray task retries and actor restarts/method retries are explicitly disabled.
- Review correction: configure the temporary authentication environment **before importing Ray**, which caches authentication state. The fresh-process guard avoids changing an existing driver's configuration. The token is private to the run, temporary and mode `0600`; no credential content is logged or committed.
- Final verification: Ruff lint passed and all 46 Python files passed formatting; `uv build` produced wheel and source distribution including the example. CI adds a dedicated Ray Core CPU job alongside the existing fast and PostgreSQL jobs. Hosted outcomes are recorded in the implementation PR checks, separately from local evidence.
- Cleanup: no temporary `kn-ray-*` directories, Ray worker/runtime processes or disposable test databases remained. The project PostgreSQL service was stopped with its named volume retained. The global token created by the initial Ray installation smoke was removed only after its creation/mtime metadata was verified unchanged; subsequent runs use ephemeral tokens only.
- Limits: single-node monotonic timestamps, logical scheduler resources rather than OS CPU enforcement, in-memory actor state, bounded input sizes and retained final report results. The batch deadline excludes runtime startup/shutdown. Abrupt OS termination can prevent cleanup. No timing/speedup SLO, durable actor recovery or production isolation is claimed.
- Next step: review MLP-006 and explain ObjectRef versus value, CPU slots versus outstanding work, task versus actor failure, and when a process pool suffices. Then proceed to MLP-007's Ray Jobs adapter and reconciliation with the durable controller.

## 2026-11-14 — MLP-007 — Ray Jobs execution and reconciliation

- Status: In review; implementation and local verification complete, learner explanation pending. Work began on top of the then-open [PR #9](https://github.com/OmniMechanica/kusanagi-nerv/pull/9); it merged during implementation as `499889d`. The branch was fast-forwarded to that main commit without changing the checked source tree; the new PR targets main.
- Evidence: **TESTED-CPU** on macOS arm64 for actual API/controller/Ray Jobs processes and PostgreSQL transactions. The fixed workload performs deterministic hashing; no ML training, checkpoint, model registration, artifact or GPU execution is claimed. The simulated executor remains explicitly selectable.
- Code: [Ray Jobs adapter](../../src/kusanagi_nerv/executors/ray_jobs.py), [dispatch storage](../../src/kusanagi_nerv/repositories/attempts.py), [migration](../../migrations/versions/0004_ray_dispatch.py), [cluster launcher](../../src/kusanagi_nerv/runtime/local.py), [dummy workload](../../src/kusanagi_nerv/workloads/dummy.py), [real E2E tests](../../tests/integration/test_ray_jobs.py), [runbook](../06-engineering/ray-jobs.md).
- Environment: CPython 3.12.16, uv 0.13.7, Ray 2.60.0, HTTPX2 2.16.0, PostgreSQL 18.6. HTTPX2 is now a runtime dependency; the optional `ray-jobs` extra installs Ray's dashboard/Jobs dependencies. Existing locked dependency versions are unchanged; the full resolution is in `uv.lock`.
- With `KUSANAGI_TEST_DATABASE_URL` exported and `ray-jobs` installed, `.venv/bin/pytest -q --tb=short`: **318 passed in 103.74s** — 243 fast cases, 65 PostgreSQL cases, 5 Ray Core cases and 5 Ray Jobs/PostgreSQL cases. The final run started/stopped its Ray Jobs cluster automatically; no pre-existing cluster was required.
- E2E evidence: a real API returns `202` with attempt zero before any controller starts. A separate controller submits to Ray, workload-start evidence advances the job, and PID/step-count/checksum persist in the attempt. The workload PID differs from API, controller and test processes.
- Recovery evidence: one controller is actually killed after observing running work. Another waits for natural lease expiry and finalizes the same attempt/submission/target with an incremented lease epoch. A separate test exits the controller immediately after submit returns, before ledger acknowledgement; Ray acceptance survives with `accepted_at` still null, and the replacement reconciles one workload execution.
- Duplicate/short-work evidence: two concurrent actual Ray POST submissions use the same identity; one workload starts/completes. Delaying the first ledger observation until after completion finalizes success without inventing a running timestamp. Metadata and entrypoint mismatches remain unknown rather than adopting another execution.
- Cancellation evidence: an event pauses HTTP submit after DB admission. HTTP cancel records intent; revocation plus Ray `404` leaves the job cancelling. Releasing submit allows eventual confirmed stop. A subsequent duplicate wire POST after terminal cancellation is rejected by the retained Ray identity. Separate real-DB barrier tests serialize dispatch/cancel and verify stale-owner rejection and target-pinned recovery.
- Failure evidence: unknown/malformed/unreachable/unauthorized responses do not become retryable execution failures. A real nonzero Ray driver exit is classified permanent, producing one failed attempt even with a larger retry budget. The adapter requires explicit future transient-error evidence before enabling those retries.
- Migration: explicit development upgrade succeeded; `alembic current` returned `0004_ray_dispatch (head)` and `alembic check` reported no new operations. Tests cover populated-ledger preservation, ordinary reversible empty/simulated schema upgrades and refusal to discard Ray attempt fences on downgrade.
- Review corrections: start/result evidence is distinct from Ray driver state; session identity and job/attempt/payload metadata are checked before adopting observations. Dispatch admission commits before HTTP, and cancellation uses the same job-first lock order. The cluster info file is published atomically without overwriting another file. Unrecognized/malformed status values stay unknown.
- Final verification: Ruff lint passed; all 56 Python files passed formatting. Lock consistency, wheel/source build and all local documentation file links passed. The CI workflow adds a fourth job for real Ray Jobs plus PostgreSQL, separate from fast checks, PostgreSQL-only cases and Ray Core. Hosted outcomes are tracked in the implementation PR checks.
- Cleanup: no temporary `kn-ray-*` runtime directories, Ray runtime/test processes or disposable databases remained. The manual smoke cluster's info file was removed on shutdown. PostgreSQL was stopped with its development volume and schema retained.
- Limits/tradeoffs: local same-host Python environment, fixed trusted dummy entrypoint, test tenant and one active attempt per controller. Ray job identity retention is required; head recreation/manual deletion is outside the guarantee. A crash between DB dispatch admission and actual sending can leave cancellation unresolved. Start/results currently depend on retained job logs. Resources are logical reservations, not OS limits. See the runbook for these boundaries; no exactly-once side-effect guarantee is claimed.
- Next step: review MLP-007 and explain the stable reconciliation key, accepted-but-unacknowledged submit, cancellation versus missing runtime state, and Ray-head loss. Then proceed to MLP-008's reproducible CPU PyTorch training loop.

## 2026-11-17 — MLP-008 — Reproducible CPU PyTorch training

- Status: In review. [PR #10](https://github.com/OmniMechanica/kusanagi-nerv/pull/10) merged MLP-007 as `ac3d8dc`. This increment starts from that commit. The learner explanation is pending.
- Evidence: **TESTED-CPU** on macOS arm64, Python 3.12.16, uv 0.13.7 and PyTorch 2.15.0. The optional `training` extra uses the official CPU wheel index and includes NumPy 2.5.5 for PyTorch initialization. Existing locked dependency versions were preserved.
- Workload: `synthetic-v1` generates 512 rows locally and splits them into 384 training and 128 evaluation rows. The 4 → 24 → 1 MLP uses Adam with MSE. Dataset/split seeds control the evaluation rows. Training seed controls model initialization and minibatch order.
- Evidence run: `python -m kusanagi_nerv.training --seed 42 --epochs 20 --batch-size 32 --learning-rate 0.01 --output <new-path>/model.pt`. Held-out MSE fell from `7.496293` to `0.103600`, held-out MAE was `0.201685`, and final training MSE was `0.045307`. The reloaded model predicted `1.550198` for `[0.25, -0.5, 0.1, 0.3]`.
- The sample artifact was 4509 bytes. Two independent runs had the same SHA-256, `8dd05ba6660a304ac91122b6500c901fcd1823f57c0cad191b0e30b18a783fda`. Tests separately verify tensor/metric equality under the same configuration and recompute the file hash from the reported path. Cross-platform and cross-version byte identity is outside the reproducibility claim.
- The CLI returns a single JSON report with an absolute path, SHA-256, completed epochs and metrics. The published file includes `state_dict` plus primitive configuration, data, environment and metric metadata. It uses strict CPU loading with `weights_only=True`. Serialization stages a private file beside the destination and publishes it as a complete file without replacing an existing artifact.
- CPU cases also verify a changed training seed, disjoint evaluation rows, metric agreement with a forward pass, output collision handling and cleanup after injected write failure. Configuration validation runs in the fast suite. CI has a dedicated PyTorch CPU job.
- With `KUSANAGI_TEST_DATABASE_URL` set and both `training` and `ray-jobs` extras installed, `.venv/bin/pytest -q --tb=short` completed with **330 passed in 126.30s** (252 fast, 65 PostgreSQL, 5 Ray Core, 5 Ray Jobs/PostgreSQL and 3 PyTorch CPU cases).
- Ruff lint and formatting, lockfile consistency, wheel/sdist build and local Markdown links passed. The built wheel contains all four training modules. See [training guide](../06-engineering/cpu-training.md), [workload](../../src/kusanagi_nerv/training/loop.py) and [CPU tests](../../tests/training/test_cpu_training.py).
- PR #11 review follow-up: initialize each model with its own CPU generator using the original `Linear` sampling bounds. The synchronized four-thread regression test failed on the original code and passed with this change. It compares every parameter to a sequential same-seed reference and checks the default CPU RNG state. All 13 focused training/config cases passed. The default seed-42 run reproduced the SHA-256 and metrics recorded above.
- Next step: explain why held-out model error and controller health are separate measurements, and which state is needed to resume an interrupted run. MLP-009 introduces Ray Train execution, followed by durable checkpoint publication and recovery in MLP-010.

## 2026-11-17 — MLP-009 — Ray Train CPU DDP

- Status: In review. Work started from the reviewed MLP-008 branch, including the private initialization RNG fix. [PR #11](https://github.com/OmniMechanica/kusanagi-nerv/pull/11) merged during implementation as `e3c08dc`, and the MLP-009 branch was fast-forwarded to that main commit.
- Evidence: **TESTED-CPU** with Ray 2.60.0, PyTorch 2.15.0 and Python 3.12.16 on macOS arm64. GPU execution remains **DESIGN-ONLY**. The optional `ray-train` extra installs Ray Train and reuses the locked CPU PyTorch extra.
- Default run: driver PID `3946`, rank 0 PID `4035`, rank 1 PID `4034`, world size `2`, backend `gloo`. Each rank received 192 training rows per epoch and performed six updates at batch size 32. At epoch 20, MSE fell from `7.496293` to `0.135955` on held-out rows. Final evaluation MAE was `0.233100`.
- Both ranks reported parameter SHA-256 `8f32e34ea03d640d4d5ca0f525625b63d0defd415428988cf043985024717c0c`. The reloaded model differed by at most `9.54e-7` on held-out predictions from standalone training with equivalent batch size 64. The attempt file has its own SHA-256, recorded in the [runbook](../06-engineering/ray-train.md).
- Start/result boundary: the start record includes submission identity and rank-zero PID and is emitted after the full DDP group and first shards are ready. A real GPU request on a CPU-only runtime reached placement and failed without a start event, success report or artifact. All Ray Train retry budgets are zero, including worker, preemption and Train-controller retries.
- Integration contract: a typed `DistributedResult` and a prefixed JSON result expose rank evidence, metrics and the final artifact path/hash. The driver publishes weights after successful `fit()`. The adapter will consume these events and publish durable resume state in MLP-010.
- Reused standalone training and artifact code. Four standalone training tests and 252 fast cases passed. The standalone seed-42 sample still produced the recorded MLP-008 artifact hash. Ruff, lockfile and wheel/sdist checks passed. A sixth CI job runs Ray Train CPU DDP against the pinned `ray-train` environment.
- `.venv/bin/pytest -q tests/ray_train --tb=short` passed both real Ray Train cases in 66.34 seconds using only the `ray-train` extra. The suite checked repeated CPU runs, serial-equivalent predictions and the placement failure with zero GPUs.
- Review: the sample tests use a single node. The CUDA path explicitly requests NCCL and a GPU for each rank. Different world sizes change the effective batch. The guide explains `torchrun`, DDP, FSDP and the checks needed for live GPU execution. The learner whiteboard checkpoint remains open.
- PR #12 review follow-up: install a CLI SIGTERM handler before creating the temporary runtime directory. The subprocess regression reproduced the leftover directory before the fix and verified cleanup afterward. The GPU case checks Ray's specific placement timeout, and the DDP case compares aggregate training MSE against the serial batch-64 reference. All three Ray Train cases passed in 64.88 seconds, and all 252 fast tests passed.

## 2026-11-17 — MLP-010 — Controller-owned S3 checkpoint recovery

- Status: [PR #13](https://github.com/OmniMechanica/kusanagi-nerv/pull/13) merged as `5dfcee7`. The learning drill remains pending.
- Evidence: **TESTED-CPU** on macOS arm64 with Python 3.12.16, Ray 2.60.0, PyTorch 2.15.0, PostgreSQL 18.6 and SeaweedFS 4.53 using its S3 API. Public MinIO image pulls were denied. Compose pins the accessible S3 server by digest and binds loopback. The recovery extra adds Boto3 1.44.5 and resolves `python-dateutil` to 2.9.0.post0 for Botocore compatibility.
- Real fault boundary: Ray Jobs launched two DDP workers. Rank zero published the complete checkpoint at epoch 2 and then received SIGKILL. Ray returned a typed actor death inside a worker health-check error, and the entrypoint exited with code 75. Worker, preemption and Train-controller retry budgets remain zero.
- Resume proof: a new Python process loaded the S3 object and reported epoch 2, two epoch records and Adam step 12. The controller created a distinct submission and attempt ID, with the old URI, SHA-256 and epoch durably stored before dispatch. Attempt 2 first ran epoch 3, published checkpoints through epoch 5, and wrote the final model under its own S3 prefix. Its predictions matched the equivalent uninterrupted batch-64 training within tolerance. The first two metric records match the original checkpoint's history.
- Retry proof: both cases inject a lost first submit response after actual Ray acceptance. PostgreSQL retains exactly one open attempt with unknown outcome until reconciliation. A budget of one permits the resume attempt; a budget of zero leaves one failed attempt, preserves the checkpoint and publishes no artifact.
- S3 proof: single conditional PUT, idempotent replay, conflicting-key rejection and SHA-256 validation of downloaded bytes were verified on the actual store. Selection is confined to the failed attempt's prefix. The worker verifies compatibility and checksum before deserializing resume state. Scope includes a same-host local store and retained object records.
- Local checks: 252 fast cases, 65 PostgreSQL regression cases, two real worker-loss cases (60.61 seconds), and the isolated S3 write/integrity case passed. The development ledger was upgraded to `0005_training_checkpoints`, and `alembic check` found no drift. Ruff, lockfile, wheel/sdist and local documentation links passed. A seventh CI job covers S3 recovery with PostgreSQL and real Ray on Linux.
- The combined local run passed all 337 tests in 238.96 seconds. The first Linux run passed both worker-loss cases but failed initial bucket setup because the S3 server closed the first authenticated request during startup. A cold local restart also exposed that `HeadBucket` can succeed before object writes are ready. The fixture now checks authenticated bucket access and round-trips a disposable object with a 30-second deadline. Authorization errors still fail immediately.
- Review follow-up persists `retry_checkpoint` independently of publication and restore inputs. A second worker loss before publishing can reuse the verified checkpoint from attempt one. Cancellation uses verified Ray Jobs status to send `stop` during an S3 outage, and success requires matching hashes for the downloaded artifact and final checkpoint. Recovery skips candidates with invalid integrity metadata or bytes. The PostgreSQL migration test confirms that a retry pending on schema `0005_training_checkpoints` retains its selected checkpoint after upgrade to `0006_retry_checkpoint`.
- The independent review follow-up keeps interrupted S3 downloads in `UNKNOWN` and treats `NoSuchKey` as confirmed absence. After a completed recovery search the same ledger transaction updates the public checkpoint URI to the verified candidate or clears it. The upgrade runbook stops all old controllers before the `0006` backfill.
- Bounded S3 reads now verify completeness before hash comparison. The adapter reuses one read-only terminal check per observed submission across poll timeouts and discards obsolete checks when moving to another submission or cancelling a failed job.
- Runbook, ledger query and learning drill: [Checkpoint recovery](../06-engineering/checkpoint-recovery.md).

## 2026-11-18 — MLP-011 — Model serving

- `0007_model_registry` adds durable model IDs, versions and SHA-256 references to verified inference artifacts. A fenced training completion registers one model and publishes it in the existing job response in a single transaction.
- PostgreSQL tests exercise rollback on an invalid digest, competing completion observations, and backfill from a terminal MLP-010 attempt. The Ray/S3 recovery cases check that the job response exposes the registered model with the exact artifact URI and checksum.
- Local checks passed: 252 fast, 68 PostgreSQL, 5 Ray Jobs and 11 S3 recovery cases. The real Ray Jobs checks preserve the dummy workload result across controller restart. Its job response has no model ID or version.
- `0008_model_deployments` persists deploy requests and the latest observed Serve state. The tenant-scoped API checks S3 bytes and model format. The deployment controller uses a stable application name and verifies its revision before recording readiness.
- **TESTED-CPU** on macOS with Ray 2.60, PostgreSQL and SeaweedFS. The controller exited after Ray accepted the deployment. A new controller recorded the same application as ready and its HTTP proxy responded from the loaded replica. A corrupt weights artifact with a valid SHA-256 returned `409`.
- Deployment verification passed 252 fast, 69 PostgreSQL, 5 Ray Jobs, 11 S3 recovery, 4 CPU training and 1 real Ray Serve case. Ruff, schema comparison, the dependency lock and package build passed.
- The serving-only environment avoids a macOS Ray dashboard subprocess abort when importing PyArrow from the training environment. See [model deployment](../06-engineering/model-deployment.md).
- **TESTED-CPU inference.** A public job request ran a real two-worker DDP training job and registered model `d2e57701-e5f6-470c-9836-98dccf6a9070` at version `v1`. Public deploy, readiness polling and prediction produced HTTP `200` and two values matching local inference on the exact saved S3 artifact.
- The prediction contract checks tenant ownership, readiness, bounded four-feature input, replica identity and response shape. The real Serve test compares concurrent predictions with local inference after restarting the deployment controller and the Serve service. The interview drill remains open in [MLP-011](tasks/MLP-011.md).
- The inference regression runs passed 342 tests across the fast, PostgreSQL, Ray Jobs, S3 recovery, CPU training and Ray Serve groups. The full DDP-to-prediction experiment ran separately.
- Astra review follow-up preserves confirmed readiness after an unrelated failed Serve PUT and commits `failed` before removing a revision. The regression simulates interruption after Ray accepts the removal and confirms that only an explicit deploy request starts a new revision. The real Serve test covers a truncated artifact returning `409` and finite predictions at the float32 input boundary.

## 2026-11-18 — MLP-013 — Public CPU pipeline

- **TESTED-CPU** on macOS with Ray 2.60, PyTorch 2.15, PostgreSQL 18.6 and SeaweedFS 4.53. `KUSANAGI_DOCKER_CONTEXT=desktop-linux UV_OFFLINE=1 scripts/test-cpu-pipeline.sh --tb=short` passed both scenarios in 133.24 seconds of pytest runtime. The [runbook](../06-engineering/cpu-pipeline.md) describes the environments, output records and cleanup.
- The ordinary job `142387db-2b33-479b-89d5-ddb2f763f297` completed on one Ray submission and served model `38fe43e9-afa7-49a3-b13c-59d08b8ee406` at version `v1`. Both scenarios obtained `202` before starting the training controller, then read the same job ID and creation timestamp from the restarted API.
- Job `06ccf896-1f7b-4e9a-ac9d-b8f65fb918f8` lost its rank-zero worker after epoch 2. Ray reported driver exit code 75 for `kusanagi-cfbc01d6-97ef-4b5e-bf96-2e397ded2e82`. Retry `kusanagi-c331c804-7031-48fb-8e8e-e0a76a4ddfb5` used the exact checkpoint reference, restored Adam step 12 and started epoch 3. A fresh Python process verified the checkpoint.
- The recovered job registered model `0afe2867-b2f5-4501-b80d-2e1ae8bf16df` at version `v1`. Both models returned HTTP `200` through the public prediction route with `[0.040756889, -0.178886503, 0.012715655, -0.036401346]` (rounded). The tests compared the values with the exact registered weights and the uninterrupted CPU reference.

The worker-loss record printed these S3 objects. The test removed its job prefix during cleanup.

```text
checkpoint s3://kusanagi-training/jobs/06ccf896-1f7b-4e9a-ac9d-b8f65fb918f8/attempts/kusanagi-cfbc01d6-97ef-4b5e-bf96-2e397ded2e82/checkpoints/0002.pt
artifact   s3://kusanagi-training/jobs/06ccf896-1f7b-4e9a-ac9d-b8f65fb918f8/attempts/kusanagi-c331c804-7031-48fb-8e8e-e0a76a4ddfb5/model.pt
```

## 2026-11-18 — MLP-015 — CPU batch and serving on kind

- **TESTED-CPU** on Docker Desktop Linux ARM64 with 4 CPUs and 8 GB memory. The complete run took 2 minutes 24 seconds including cluster creation and teardown. The script used kind 0.31.0, Kubernetes and kubectl 1.35.0, Helm 3.18.3, KubeRay 1.6.2 and the Ray 2.60.0 Python 3.12 CPU image. [Script](../../scripts/test-kuberay-kind.sh) · [Runbook](../06-engineering/kuberay-kind.md).
- Strict server-side dry-run accepted the ServiceAccount, generated ConfigMap, RayCluster, RayJob and RayService. Both Ray clusters reached one ready worker each. The RayJob's Kubernetes submitter Pod completed and the operator reported `SUCCEEDED` / `Complete`. RayService reached `Ready` with two Serve endpoints.
- The driver ran on `cpu-batch-head-c7cvv` and both tasks ran on `cpu-batch-cpu-worker-gzn44`. The Serve ClusterIP endpoint `cpu-predictor-serve-svc:8000/predict` returned HTTP `200` from `cpu-predictor-pd8ds-cpu-worker-4pcd5`. Both paths reported model `linear-cpu-v1` and predictions `[1.0, 3.0]`.
- Live validation exposed three configuration limits. The initial head requests of 500m left insufficient allocatable CPU for the second worker after system reservations. The operator required the `ray.io/cluster` key even though the CRD accepted a different map key. The injected Serve worker readiness probe needed a local proxy, so the demo uses `EveryNode`. The smoke client makes the internal HTTP request using the Python standard library available in the image.
- Local checks passed for 252 fast tests, Ruff lint and formatting, Bash syntax and runbook links. The demo uses fixed weights. The full platform's Kubernetes targets, artifact access identity and ownership boundaries are **DESIGN-ONLY**. The independent MLP-014 mock is in progress.

## 2026-11-18 — MLP-016 — GPU resource contract

- **DESIGN-ONLY** for NVIDIA device injection, CUDA, NCCL and GPU performance. The [GPU RayJob](../../deploy/gpu/ray-job.yaml) and [architecture](../02-architecture/gpu-kubernetes.md) specify one exclusive device per worker, a CPU head and a device-visibility probe. The pinned Ray 2.60.0 Python 3.12 CUDA image index contains Linux AMD64 and ARM64 manifests.
- **TESTED-CPU** for Kubernetes validation and zero-capacity scheduling on kind 0.31.0, Kubernetes 1.35.0 and KubeRay 1.6.2. `kubectl apply --dry-run=server --validate=strict -k deploy/gpu` accepted the ServiceAccount, generated ConfigMap and RayJob.
- A native Pod rendered from the GPU worker template passed strict server-side dry-run with one GPU in requests and limits. Setting its test copy to request one and limit two was rejected with `must be equal to nvidia.com/gpu limit of 2`.
- The disposable node had the required pool label and zero allocatable GPUs. Applying the unchanged worker Pod template produced `Pending` and the scheduler event `0/1 nodes are available: 1 Insufficient nvidia.com/gpu`. Only the native Pod was applied for this check.
- The design covers quota holds, bounded backfill, same-node placement, independent scaling loops and separate attribution of allocated GPU-hours and utilization. Live training and GPU measurement belong to [MLP-022](tasks/MLP-022.md).

## 2026-11-18 — Astra review follow-up for MLP-011 and MLP-013–016

- The deployment reconciler checks the effective Serve proxy configuration. A confirmed listener mismatch moves a ready model to `deploying`. Artifact admission and replica initialization reject weights containing NaN or Inf.
- A training success observation without the typed result leaves the attempt recoverable. Migration `0009_job_model_result` repairs legacy result links and enforces a match on the model ID, source job, tenant and version. PostgreSQL tests cover old-schema repair, a competing completion and invalid links at commit.
- The CPU runner discovers the published PostgreSQL and S3 ports unless explicit URLs are configured. The live test used custom ports supplied through a Compose environment file. Both public CPU cases returned HTTP `200`. The interrupted case restored Adam step 12 and resumed at epoch 3.
- The live kind smoke test returned `[1.0, 3.0]` for the baseline and HTTP `422` for an overflowing finite batch. The GPU probe now requires exactly one device enumerated by the CUDA driver. Five simulated driver cases covered zero and multiple devices, initialization and enumeration failures, and the successful result. Physical GPU execution remains **DESIGN-ONLY**.
- Verification passed 252 fast tests, 32 CPU E2E and PostgreSQL integration cases, and the separate live Serve test with both nonfinite artifacts. The database checks included Alembic schema comparison.

The [backlog CSV](backlog.csv) also tracks ticket statuses and dependencies. Keep both consistent as tasks complete.
