# Test strategy and reproducibility

## Test pyramid

| Level | Scope | Earliest ticket |
|---|---|---|
| Unit | State transitions, Pydantic, retry policy, quota arithmetic | MLP-001 |
| API contract | Create/get validation and 202/404/422 in MLP-002; idempotency/409 and cancellation in MLP-005 | MLP-002 |
| DB integration | Real PostgreSQL, transaction races, migrations | MLP-003 |
| Controller integration | Multi-controller claim, restart and reconciliation | MLP-004 |
| Ray integration | Runtime adapter executes outside API | MLP-006 |
| Training recovery | Fault injected mid-train; checkpoint resumes | MLP-010 |
| Full E2E | Submit → train → restore → serve → predict | MLP-013 |
| Chaos | Controller/worker/object-store failure matrix | MLP-020 |

## Non-flaky testing principles

- MLP-001 covers all 64 state pairs against the specification: 18 allowed, 46 forbidden. Expected outcomes must not be generated from the implementation map. These tests establish graph rules, not concurrency safety.
- Prefer controllable clocks, events, barriers and deterministic seeds to arbitrary `sleep` calls.
- Clean test data after each integration run; use isolated test DB/schema.
- Pin/record environment and runtime versions for Ray/PyTorch; platform compatibility varies.
- Mark GPU tests and resource-heavy tests separately; don't block basic local tests on missing GPU.
- Record actual commands and observed outputs. A green mocked test is not hardware proof.

## Critical race tests

1. Two POST requests share idempotency key/payload; one logical job created.
2. Same key/different payload returns conflict.
3. Two controllers claim the same queue at once; only one active lease is authoritative.
4. Cancel arrives near completion; final state matches observed task outcome.
5. Controller restarts between Ray submit and state write; reconciles instead of multiplying runs.
6. Worker fails after checkpoint; attempt N+1 resumes from saved state.
7. Tenant A cannot get/cancel a B job.
8. GPU-only request on CPU cluster is not silently converted to CPU.
9. Submit is accepted, its response is lost and cancel arrives while still `QUEUED`; the job remains `CANCELLING` until safe termination is confirmed.
10. Executor lookup is `UNKNOWN`, or `NOT_FOUND` while submit is still in flight; do not falsely finalize or allocate a new attempt. A stale lease holder cannot commit.
11. A short execution completes between polls; observed success/failure can finalize from `QUEUED` without a fabricated running timestamp.
12. Two controllers claim different jobs with one tenant slot remaining; only one reservation succeeds (MLP-017).
13. Repeated successful-job reconciliation creates one model; repeated deploy/controller restart preserves desired deployment identity. HTTP deploy/readiness/predict works without the SDK (MLP-011/013).

The worker-failure recovery experiment explicitly disables Ray Train's internal recovery and verifies the controller's attempt budget and selected checkpoint. See [retry ownership](../02-architecture/reliability.md#retry-ownership).

## Reporting

In each task/PR record test command, environment (Mac CPU, local kind, GPU cloud, simulated), summary and limits. Keep a 10-minute reproducible end-to-end demo before adding optional features.
