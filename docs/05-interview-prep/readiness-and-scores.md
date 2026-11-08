# Interview preparation and scoring

## Four checkpoints

| When | Interview exercise | Pass evidence |
|---|---|---|
| Day 7 | 30-min Python + Ray walkthrough | Async vs CPU, idempotency, safe controller claim, retries |
| Day 14 | 45-min Python task + 45–60-min design | CPU E2E demo; defend Ray/ML lifecycle; list 3 design weaknesses |
| Day 20 | Full failure scenario + 60-min design | Reconcile after crash, checkpoint restore, GPU/fairness design |
| Day 21 | 10-min project demo + behavioral mock | Structured technical explanation, evidence-backed stories |

## System design scorecard

Each criterion is 0–4, total max 24. Score is an **internal preparation heuristic**, not Revolut's hiring criteria.

| Criterion | 0 | 2 | 4 |
|---|---|---|---|
| Requirements | No clarification | Main user flows | SLOs, scale, constraints, failure budget |
| Architecture | Unclear | Working architecture | Clear boundaries, alternatives, evolution |
| Python | Incorrect | Works | Type-safe, testable, race/error-aware |
| ML/GPU | Vague | Basic training lifecycle | GPU placement, DDP/FSDP, checkpoints, inference |
| Reliability | Happy path | Basic retries | Reconciliation, idempotency, failure drill, telemetry |
| Communication | Disorganized | Mostly coherent | Concise, explicit tradeoffs and evidence |

**Target:** ≥19/24 and no category below 2. Revisit weak categories before adding stretch features.

## Practice format

- 5 min: ask clarifying questions and estimate scale.
- 10 min: define APIs and data model.
- 15 min: architecture and critical paths.
- 15 min: faults, GPU scheduling, multi-tenancy and cost.
- 10 min: explain alternatives, incremental rollout and test strategy.
- 5 min: summarize tradeoffs and key metrics.

## Answering honestly

Talk about verified DataRobot GPU/Python/ML platform and automated job recovery. Explain OpenVPN's Kubernetes/IDP strengths and indirect GPU usage. Do not claim unverified historical experience with Ray/Flyte/DDP/checkpoint formats; show the current capstone when asked for those technologies.
