# Scope control and evidence rules

## Delivery slices

| Slice | Core capability | Demonstration |
|---|---|---|
| Foundation | Domain state machine, FastAPI, PostgreSQL, dispatcher | Job survives API restart and transitions atomically |
| Working MVP | Ray, PyTorch, training, checkpoints, Ray Serve | End-to-end CPU scenario and real failure recovery |
| Production-aware | KubeRay/GPU, multi-tenancy, monitoring, cost | Manifests, controlled tests, system design and tradeoffs |
| Stretch | Real GPUs, multi-node DDP/FSDP, MIG, Flyte PoC | Only when hardware/time permit; label measurements explicitly |

## Evidence classification

Use these labels in pull requests, docs, and interviews:

- `TESTED-CPU`: executed locally or in CI on CPU, with command and logs.
- `TESTED-GPU`: executed on GPU hardware, including device/runtime metadata.
- `SIMULATED`: verified by a controlled fake/mock; not a production measurement.
- `DESIGN-ONLY`: reviewed configuration, diagram or reasoning; not executed.
- `NOT-STARTED`: planned only.

Each task should record evidence in [progress.md](../03-planning/progress.md) or its PR, including environment, command/test and result.

## Honest external positioning

DataRobot work included hands-on Python engineering around GPU-backed ML training/inference infrastructure and automated job recovery. OpenVPN platform work covered large-scale Kubernetes and developer experience, with GPU resources primarily consumed rather than GPU platform ownership. Do not attribute unverified implementation details (Ray, Flyte, checkpoint format, DDP, etc.) to either employer. The capstone establishes **new hands-on practice**.

## Tradeoffs to articulate

- Database-backed queue vs dedicated broker; avoiding duplicate execution vs impossible exactly-once claims.
- Async API vs CPU-heavy background work; autoscaling vs quota/fairness.
- Ray execution vs Flyte workflow orchestration.
- Local CPU functional testing vs production GPU networking and scheduling risks.
- Warm inference replicas vs scale-to-zero cold-start costs.
