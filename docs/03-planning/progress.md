# Progress tracker and evidence journal

**Last updated:** 2026-11-09  
**Status:** MLP-001 merged and technically approved, with its learning checkpoint pending. MLP-002 In review — create/get API implemented and locally verified. Persistence and execution remain future milestones.

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
| 02 | MLP-002 | In review | [API evidence](#2026-11-09--mlp-002--typed-job-api) | Review create/get API and rehearse 202/async/durable-queue tradeoffs |
| 03 | MLP-003 | Planned | — | — |
| 04 | MLP-004 | Planned | — | — |
| 05 | MLP-005 | Planned | — | — |
| 06 | MLP-006 | Planned | — | — |
| 07 | MLP-007 | Planned | — | — |
| 08 | MLP-008 | Planned | — | — |
| 09 | MLP-009 | Planned | — | — |
| 10 | MLP-010 | Planned | — | — |
| 11 | MLP-011 | Planned | — | — |
| 12 | MLP-012 | Planned | — | — |
| 13 | MLP-013 | Planned | — | — |
| 14 | MLP-014 | Planned | — | — |
| 15 | MLP-015 | Planned | — | — |
| 16 | MLP-016 | Planned | — | — |
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

The [backlog CSV](backlog.csv) also tracks ticket statuses and dependencies. Keep both consistent as tasks complete.
