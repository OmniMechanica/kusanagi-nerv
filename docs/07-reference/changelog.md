# Documentation change log

## 2026-11-11 — v0.8 MLP-004 durable controller

- Added a separate controller, durable attempt/submission identities, PostgreSQL queue claims and owner/epoch/expiry guards; generic state updates cannot bypass an existing attempt.
- Added an explicit persistent simulation adapter and a run/recovery guide separating real DB evidence from simulated execution.
- Recorded 204 passing tests, including two competing claim transactions, stale and blocked writers, uncertain submission outcomes and an actual controller process crash/restart.
- Moved MLP-004 to In review with its learning drill pending. Recorded merged PR #5/#6 and integration of MLP-003 from its merged feature branch into the new main-targeted branch.
- Retry policy, cancellation and HTTP idempotency remain MLP-005; no Ray/training/GPU execution is claimed.

## 2026-11-11 — v0.7 MLP-003 PostgreSQL ledger

- Added transactional async PostgreSQL storage, an explicit Alembic migration, canonical payload hashes and conditional status/version updates.
- Made the database URL required for the normal API, retained explicit memory injection for tests, and documented sanitized `503` errors and uncertain commit outcomes.
- Added Docker Compose setup, migration/test runbook and a separate PostgreSQL CI job; recorded 174 passing tests including a real API process crash and competing writers.
- Moved MLP-003 to In review, preserving the still-open MLP-002 dependency and pending learning checks. Defined the next attempt/lease migration without claiming dispatch or remote cancellation safety.

## 2026-11-09 — v0.6 MLP-002 typed job API

- Added typed FastAPI create/get routes, strict Pydantic request validation, documented error envelopes and a tenant-scoped injectable in-memory repository.
- Kept domain records independent of HTTP/storage libraries and made validation ceilings, defaults and partial milestone limitations explicit.
- Recorded 146 passing tests, lint/format checks, package builds and a real loopback Uvicorn smoke; added local API/curl instructions.
- Moved MLP-002 to In review. Recorded MLP-001 merge/technical approval while preserving its pending learning checkpoint; deferred static analysis to the P1 tooling follow-up.

## 2026-11-09 — v0.5 MLP-001 lifecycle implementation

- Implemented the eight-state lifecycle, immutable transition map, pure transition functions and structured invalid-transition error without runtime dependencies.
- Added exhaustive state-pair checks, named lifecycle paths and explicit invalid-input behavior; recorded 79 passing tests, lint/format checks and successful package builds.
- Added design rationale for retry attempts, terminal outcomes, strict enum arguments and later cancellation guards.
- Synchronized current task/index/backlog status to In review. Guided implementation is complete; the learner's design explanation remains pending.

## 2026-11-08 — v0.4 Python bootstrap

- Added a packaged `src/` layout, pinned Python interpreter, uv lockfile, pytest/Ruff configuration and initial CI workflow.
- Documented setup/check commands and a passing package-installation smoke test; left the lifecycle implementation and domain tests as developer exercises.
- Moved MLP-001 to In progress with local verification evidence. No platform feature or completed lifecycle implementation is claimed.

## 2026-11-08 — v0.3 Vendor-neutral interview guidance

- Made the target role, interview scorecard and scaling prompts vendor-neutral; removed the company-specific vacancy link.

## 2026-11-08 — v0.2 Contract review and implementation readiness

- Kept the eight job states and added queued cancellation, pre-running failure/retry and missed-running-observation success paths. Distinguished pure graph checks from persistent execution guards; MLP-001 now covers all 64 state pairs.
- Defined a canonical CreateJob schema without tenant input, explicit cancellation responses, result model ID/version, model readiness and the P0 self-service deploy/predict flow.
- Selected Ray Jobs for the durable adapter, with submission identity persisted before dispatch, explicit unknown/not-found semantics and controller-owned retry budget/checkpoint selection.
- Added active execution reservation semantics and the cross-controller tenant-cap race case for MLP-017.
- Moved server-side model registration/deployment into MLP-011; MLP-012 now wraps the completed API as an optional SDK.
- Corrected recovery/chaos/final-rehearsal dependencies, kept GPU design on the final P0 path, and separated optional full Gate C from Gate D. Synchronized task cards, CSV, indexes and demo guidance.
- Added a bootstrap pre-step inside MLP-001 for the actual empty application repository, with a Python/package/tooling baseline and early lint/unit CI. No new ticket IDs were added.
- Documented the 57–81.5-hour task budget plus 0.5–1-hour bootstrap allowance and the need to replan against available hours.
- Documentation changes only. All 26 implementation tickets remain Planned; no application code, environment setup, runtime tests or CI execution is claimed by this revision.

## 2026-11-08 — v0.1 Planning baseline

- Renamed the capstone product to **KUSANAGI NERV — 草薙・迷走神経**, tagline *The Wandering Nerve of Distributed Intelligence*.
- Split the original 21-day plan and 26 tickets into navigable sections.
- Mapped four CourseHunter sources and official technical references to engineering tasks.
- Defined explicit P0/P1/P2 priorities, verification gates and evidence standards.
- Added API/state contracts, architecture, reliability/GPU/tenancy design and decision records.
- Introduced individual tasks with dependencies, implementation steps, acceptance checks and drills.
- Documentation only; **no local repository implementation has been modified**.

Follow-up changes should record affected tasks, scope changes and decisions.
