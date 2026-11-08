# AI Platform Engineer: requirement-to-evidence matrix

Target role: **AI Platform Engineer — Serverless Training and Inference**. This vendor-neutral matrix defines the capstone's preparation goals.

| Job requirement | Priority | Concrete proof in project | How to defend it |
|---|---|---|---|
| Strong Python backend engineering | P0 | FastAPI, Pydantic, typing, SQLAlchemy, pytest, async boundaries, SDK | Code review: DI, races, idempotency, API semantics |
| Self-service training/inference | P0 | POST jobs + Python SDK, deploy + predict | Live E2E demo without kubectl |
| Flyte, Ray or equivalent | P0 Ray / P1 Flyte comparison | Ray Core/Train/Serve, comparison ADR | Why Ray vs Flyte? When use both? |
| Kubernetes GPU scheduling / allocation | P0 design, P1 live test | `nvidia.com/gpu` manifest, KubeRay RayJob, quota examples | Device plugin, bin packing, MIG, time slicing |
| Distributed training | P0 | Ray Train multi-worker CPU + GPU optional; PyTorch DDP explanation | DDP vs FSDP; world-size; synchronization |
| Queueing / autoscaling | P0 queue / P1 autoscale | Durable claim/reconcile, tenant quotas, Serve replicas, KubeRay config | Backpressure, demand metrics, scale-to-zero latency |
| Reliable lifecycle: checkpoints/retries/recovery/cancel | P0 | Attempt log, failure drill, resume at epoch N | Worker vs driver failure; exactly-once limitations |
| Monitoring, isolation, cost visibility | P0 basic / P1 advanced | Prometheus metrics, tenant tests, GPU-hour estimate | SLO, cost allocation, cardinality and isolation |
| CS fundamentals, engineering practice | P0 | State machine, typed interfaces, tests, code reviews, CI | Concurrency, algorithmic tradeoffs, operational ownership |

## Experience stories to prepare

- DataRobot: Python/GPU ML training & inference platform; orchestration, operator work, real failure recovery — tell the honest design tradeoffs and your personal contribution.
- OpenVPN: self-service developer platform, Kubernetes at scale, high availability and leadership; GPU consumption, **not** direct GPU scheduler ownership.
- Capgemini: ten engineers, GitOps for 200+ services, stakeholder architecture decisions.

Avoid claiming Flyte/Ray were used at DataRobot if the previous stack was different. The capstone supplies current hands-on Ray evidence, not retroactive commercial claims.
