# 21-day incremental delivery roadmap

**Priority legend:** P0 = essential to prove the vacancy's core skills; P1 = improves production realism and interview performance; P2 = stretch, skip if time is short. A day is a learning *slice*, not a promise everything takes three hours. Move unfinished work forward based on the critical path. Use the failure drill and system design as capstone gates.

Daily pattern: **50–60 min learning + 90–110 min building + 20–30 min explaining/review**.

Before the state-machine exercise, complete [bootstrap](../06-engineering/bootstrap.md) inside `MLP-001` (an additional 0.5–1 hour). Scheduled task estimates total 57–81.5 hours, versus 42–63 hours of availability at 2–3 hours/day; see [budget and replanning](README.md#estimates-and-availability). Learning is already included in task estimates. Reserve time for integration and move P1 work or dates when necessary.

| Day | Ticket | Priority | Small increment | Acceptance / interview proof |
|---|---|---|---|---|
| 01 | MLP-001 | P0 | Bootstrap, then pure Python JobStatus transitions, errors, pytest | All 64 state pairs checked; explain cancellation outcomes and later contextual guards |
| 02 | MLP-002 | P0 | Pydantic JobSpec, FastAPI `POST`/`GET` with in-memory repo | 202 validation, OpenAPI, 400/422 and 404 tests |
| 03 | MLP-003 | P0 | Async PostgreSQL/SQLAlchemy + Alembic migration | Restart API, job record survives; tests exercise real DB |
| 04 | MLP-004 | P0 | Separate controller, claim queued jobs transactionally | Two controllers cannot claim same pending record concurrently |
| 05 | MLP-005 | P0 | Idempotency, attempts, bounded retry, cancellation | Same key same job; failure/retry/cancel integration tests |
| 06 | MLP-006 | P0 | Ray Core remote tasks and actors, CPU mode | Task executes outside HTTP; measured concurrency/backpressure |
| 07 | MLP-007 | P0 | Pluggable Ray adapter + first E2E dummy job | POST -> QUEUED -> RUNNING -> SUCCEEDED across processes |
| 08 | MLP-008 | P0 | PyTorch tiny MLP + synthetic data, CPU training | Reproducible model, metrics and deterministic seed |
| 09 | MLP-009 | P0 | Ray Train multi-worker execution, distributed semantics | Multi-worker CPU demo; explain DDP + GPU migration |
| 10 | MLP-010 | P0 | S3/MinIO checkpoints and recovery test | Stop worker mid-job; resume saved epoch/state |
| 11 | MLP-011 | P0 | Ray Serve inference and self-service model deployment | Deploy through API; `/predict` returns versioned output from a successful artifact |
| 12 | MLP-012 | P1 | Python SDK for the platform API | Typed client and contract tests; HTTP flow also works without SDK |
| 13 | MLP-013 | P0 | End-to-end CPU demo and end-to-end tests | Submit -> train -> checkpoint -> serve -> predict |
| 14 | MLP-014 | P0 | Week-2 mock: 1 Python challenge + 1 architecture design | Explain design without notes, identify 3 weaknesses |
| 15 | MLP-015 | P0 | KubeRay RayJob/RayService/cluster manifests on kind | Manifests validated; CPU cluster works if environment allows |
| 16 | MLP-016 | P0 | GPU scheduling spec, device plugin, MIG vs time slice | Correct `nvidia.com/gpu` resources and topology tradeoff; optional actual GPU run |
| 17 | MLP-017 | P1 | Tenant auth mock, quota, fairness, isolation tests | Team A cannot fetch B's jobs; GPU quota / active execution reservations enforced |
| 18 | MLP-018 | P1 | Prometheus metrics, load tests, Ray Serve autoscaling, cost estimates | Queue depth, latency, retries, allocation cost dashboard/metrics |
| 19 | MLP-019 | P1 | Flyte vs Ray ADR, expanded CI, security/ops hardening | Defend tradeoffs; add DB/API coverage to initial lint/unit CI |
| 20 | MLP-020 | P0 | Chaos drill + 60-min ML platform system design | Failure injection observed, validated recovery, design scored |
| 21 | MLP-021 | P0 | Final demo, coding mock, STAR stories from DataRobot | 10-min demo + 45-min Python exercise + 45-min design panel |

## P2 / stretch (only if all essential gates are done)

- MLP-022: run on a **real** GPU with Ray Train and validate `nvidia-smi` / DCGM throughput and memory; compare CPU/GPU.
- MLP-023: true multi-node GPU DDP, NCCL and FSDP memory-sharding test; expensive and requires real GPU hardware.
- MLP-024: Flyte proof-of-concept for comparable workflow; contrast with Ray controller and explain when workflow orchestration should be external.
- MLP-025: resource fragmentation / MIG experiments and GPU node autoscaling/cost tuning with cloud infra.
- MLP-026: genuine scale-to-zero for inference with cold-start tradeoffs; not required for MVP.

## Gates

### Gate A: Foundations (end Day 5)

- Types, state transition tests, REST contracts, persisted ledger, idempotency and worker separation.

### Gate B: Working CPU platform (end Day 13)

- `submit -> train -> checkpoint -> restore -> serve -> predict` works; tests prove that HTTP requests never block on training.

### Required GPU design (target Days 15–16)

- `MLP-015–016`: KubeRay manifests and GPU scheduling **understood and documented**. Design-only evidence is allowed; a live GPU test is optional. This P0 knowledge is required for the final rehearsal, but does not block the CPU chaos drill.

### Gate C: Full production-aware extension (optional; target Day 19)

- Required GPU design plus tested tenant isolation/quotas, metrics/cost estimates, expanded CI and reviewed ADRs from `MLP-017–019`.
- If P1 scope is deferred, record it and leave full Gate C incomplete. Its completion is not a prerequisite for Gate D.

### Gate D: Interview-ready (end Day 21)

- CPU failure drill, coding mock, GPU/system-design evidence from `MLP-016`, evidence-based DataRobot stories, and a clean repo/readme. The core demo uses HTTP or the optional SDK; live tenant/metrics extensions require their own completed evidence.

## 14-day compression

If an interview starts early, prioritize **MLP-001–011 + MLP-013 + MLP-014**; `MLP-012` is optional. Cover KubeRay/GPU fundamentals from `MLP-015–016` as **design-only** evidence for interview discussion. Tenant/monitoring discussions may also be design-only, with their P1 implementation explicitly deferred and full Gate C left incomplete. Replan against available hours; checkpoint recovery and a working CPU demo take priority over optional infrastructure.
