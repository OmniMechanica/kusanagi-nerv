# Progress tracker and evidence journal

**Last updated:** 2026-11-08  
**Status:** MLP-001 In progress — bootstrap verified locally; domain implementation and all platform features remain pending.

## Checkpoint gates

- [ ] **Gate A / Day 5:** typed API + durable ledger + controller + idempotency/cancellation tests
- [ ] **Gate B / Day 13–14:** CPU end-to-end training → checkpoint restore → inference; Python + architecture mock
- [ ] **Gate C / target Day 19 (optional full extension):** KubeRay/GPU design, tested tenant policy, metrics/cost, expanded CI and ADRs
- [ ] **Gate D / target Day 21:** CPU chaos evidence, demo, coding mock, KubeRay/GPU design (`MLP-015–016`), system design and STAR stories

Gate D can proceed without full Gate C. Record deferred P1 work explicitly and leave Gate C unchecked until all its criteria pass. SDK, live tenant controls and advanced metrics are optional demo extensions; design discussions do not prove implementation.

## Daily tracking

| Day | Ticket | Status | Evidence link / command | Blocker / next action |
|---:|---|---|---|---|
| 01 | MLP-001 | In progress | [Bootstrap evidence](#2026-11-08--mlp-001--repository-bootstrap) | Implement JobStatus, can_transition and independent domain tests |
| 02 | MLP-002 | Planned | — | — |
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

The [backlog CSV](backlog.csv) also tracks ticket statuses and dependencies. Keep both consistent as tasks complete.
