# Task index

**26 scoped tasks:** 21 scheduled slices and 5 optional stretch experiments.

Use these as independently reviewable engineering increments. Keep ticket IDs stable in commits and progress updates.

| Ticket | Day | Priority | Outcome |
|---|---:|---|---|
| [MLP-001](MLP-001.md) | 1 | P0 | Pure Python job state machine |
| [MLP-002](MLP-002.md) | 2 | P0 | Typed FastAPI job contracts and endpoints |
| [MLP-003](MLP-003.md) | 3 | P0 | Durable asynchronous PostgreSQL job ledger |
| [MLP-004](MLP-004.md) | 4 | P0 | Durable job controller and safe queue claiming |
| [MLP-005](MLP-005.md) | 5 | P0 | Idempotency, bounded retries and cancellation |
| [MLP-006](MLP-006.md) | 6 | P0 | Ray Core tasks, actors and backpressure |
| [MLP-007](MLP-007.md) | 7 | P0 | Ray execution adapter and job reconciliation |
| [MLP-008](MLP-008.md) | 8 | P0 | Reproducible PyTorch training loop |
| [MLP-009](MLP-009.md) | 9 | P0 | Ray Train multi-worker training and DDP concepts |
| [MLP-010](MLP-010.md) | 10 | P0 | Checkpoint storage and real recovery experiment |
| [MLP-011](MLP-011.md) | 11 | P0 | Ray Serve inference and self-service model deployment |
| [MLP-012](MLP-012.md) | 12 | P1 | Python SDK for the platform API |
| [MLP-013](MLP-013.md) | 13 | P0 | End-to-end CPU training → restore → serving test |
| [MLP-014](MLP-014.md) | 14 | P0 | Python backend mock and ML system-design checkpoint |
| [MLP-015](MLP-015.md) | 15 | P0 | KubeRay and Kubernetes deployment artifacts |
| [MLP-016](MLP-016.md) | 16 | P0 | GPU scheduling, capacity and topology design |
| [MLP-017](MLP-017.md) | 17 | P1 | Multi-tenancy, authorization and quota fairness |
| [MLP-018](MLP-018.md) | 18 | P1 | Prometheus metrics, autoscaling signals and cost model |
| [MLP-019](MLP-019.md) | 19 | P1 | Architecture decisions, Flyte comparison, CI and security |
| [MLP-020](MLP-020.md) | 20 | P0 | Controlled chaos drill and 60-minute platform design mock |
| [MLP-021](MLP-021.md) | 21 | P0 | Final demo and full interview rehearsal |
| [MLP-022](MLP-022.md) | Stretch | P2 | Actual GPU train run and telemetry |
| [MLP-023](MLP-023.md) | Stretch | P2 | Multi-GPU DDP/FSDP and NCCL experiment |
| [MLP-024](MLP-024.md) | Stretch | P2 | Flyte workflow proof of concept |
| [MLP-025](MLP-025.md) | Stretch | P2 | GPU MIG, fragmentation and scale economics |
| [MLP-026](MLP-026.md) | Stretch | P2 | Inference scale-to-zero and cold-start benchmark |

**Start:** [MLP-001](MLP-001.md), including its [bootstrap pre-step](../../06-engineering/bootstrap.md); no additional ticket is introduced.

**Tracking:** [Backlog CSV](../backlog.csv) and [Progress journal](../progress.md).
