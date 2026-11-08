# Full priority model and scope policy

Priorities are based on (1) role relevance, (2) likelihood of interview probing, (3) personal refresh needs and (4) what can be *proved* in 2–3 weeks.

| Area | Priority | Build proof | Interview proof |
|---|---|---|---|
| Typed Python, `asyncio`, testing, API design | P0 | FastAPI + Pydantic + pytest + persistence; initial lint/unit CI | Implement and defend concurrency/errors |
| Durable job lifecycle, retries, cancellation | P0 | State machine + controller + attempts + reconciliation | Explain races and at-least-once effects |
| Distributed execution | P0 | Ray remote tasks, actors, adapter | Explain resources, workers and backpressure |
| Distributed training | P0 | PyTorch + Ray Train multi-worker CPU | Explain DDP, FSDP and NCCL tradeoffs |
| Fault tolerance | P0 | Real MinIO/S3 checkpoint restore test | Failure walkthrough and proof |
| Model serving | P0 | Self-service deploy API, Ray Serve inference, versioned artifact | Latency, rollout and autoscaling |
| Kubernetes / GPU scheduling | P0 design | Manifests + GPU capacity explanation | Device plugins, MIG, topology and quotas |
| System design / communication | P0 | Diagrams, tradeoffs, mock scores | 60-minute design interview |
| Python SDK | P1 | Submit/get/cancel/predict client | Developer experience and API ergonomics |
| Multi-tenancy and auth | P1 | Cross-tenant tests, quota enforcement | Fairness/isolation tradeoffs |
| Metrics, cost, expanded CI/security | P1 | Prometheus metrics, estimates, DB/API CI and security review | SLO, capacity and operational risks |
| Flyte workflow | P2 | Optional small DAG | Contrast Ray vs Flyte |
| Real GPU DDP/FSDP / MIG / scale-to-zero | P2 | Optional measured hardware experiments | Speak carefully about untested conditions |

## Delivery policy

1. P0 dominates P1/P2 unless P1 unblocks a P0 demo (e.g., a minimal SDK is useful but not essential to show API flow).
2. Each completed ticket has observable proof. Configuration-only deliverables are labelled `DESIGN-ONLY`.
3. Skip large model training or GPU spending until the tiny CPU model and checkpoint recovery are correct.
4. Flyte is a tradeoff topic first; using Ray does not automatically teach Flyte.
5. If the interview arrives early, switch to [14-day compression](roadmap.md#14-day-compression) and run mocks earlier.
6. P1 implementation does not block the CPU chaos drill or final rehearsal. The final P0 path still includes KubeRay/GPU design (`MLP-015–016`); unexecuted GPU behavior is labelled `DESIGN-ONLY`. Leave full Gate C incomplete when P1 criteria are deferred, even if Gate D passes.
