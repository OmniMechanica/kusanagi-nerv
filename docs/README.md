# KUSANAGI NERV — Project Documentation

**草薙・迷走神経**  
*The Wandering Nerve of Distributed Intelligence*

**Product:** Custom Self-Service Serverless ML Training & Inference Platform  
**Purpose:** Target a 14-day CPU MVP + 7-day GPU/system-design preparation and optional production hardening for a Senior AI Platform Engineer interview.  
**Window:** 9–29 November 2026 (target learning slices; replan scope/dates against available hours).  
**Status:** MLP-001–004 are integrated into main through merged PRs; learning checkpoints remain pending. MLP-005 is In review with durable idempotency, bounded retries and confirmed cancellation in the simulator. MLP-006 onward remain Planned. Locally: 244 passing tests, including 57 real PostgreSQL cases; execution remains SIMULATED. See [progress](03-planning/progress.md).

## Start here

1. Read [Vision and success criteria](00-foundation/vision-and-success.md) (5 minutes).
2. Review [role/skill alignment](00-foundation/role-skills-matrix.md) and [priority policy](03-planning/priorities.md).
3. Follow the [21-day roadmap](03-planning/roadmap.md) and the [task backlog](03-planning/README.md).
4. Review the [MLP-001 lifecycle](01-product/job-lifecycle.md#mlp-001-implementation-notes), [MLP-002 API](03-planning/tasks/MLP-002.md) and [MLP-003 persistence](03-planning/tasks/MLP-003.md). Follow the [PostgreSQL guide](06-engineering/postgres.md) and [controller simulation guide](06-engineering/controller.md) to run the current increment locally, then complete the learning checkpoints before marking the tasks Done.
5. Study the relevant part of the [parallel course plan](04-learning/course-mapping.md), then rehearse a relevant [interview question](05-interview-prep/question-bank.md).
6. Track completion in [progress](03-planning/progress.md) and record evidence, not just check marks.

## Documentation map

```text
docs/
├── README.md                           # This index
├── 00-foundation/                     # Vision, scope, skills-to-evidence
├── 01-product/                         # Functional requirements, API, state machine
├── 02-architecture/                    # Planes, persistence, reliability, GPU/K8s
│   └── adr/                           # Explicit architectural decisions
├── 03-planning/                        # Priorities, roadmap, progress, backlog
│   └── tasks/                         # MLP-001 ... MLP-026 task specs
├── 04-learning/                        # Four CourseHunter courses + official docs
├── 05-interview-prep/                  # Coding, GPU/system design, stories, demos
├── 06-engineering/                     # DoD, testing, workflow, risks
└── 07-reference/                       # Glossary and terminology
```

## Delivery milestones

| Gate | Day | Required evidence |
|---|---:|---|
| A — Durable backend | 5 | Job state machine, typed API, DB persistence, dispatcher, cancellation/retry tests |
| B — CPU ML platform | 13–14 | Submit → train → checkpoint → recover → serve → predict, with test evidence |
| C — Full production-aware extension (optional) | 19 | KubeRay/GPU design plus tested P1 tenant controls, metrics/cost, expanded CI and tradeoff ADRs |
| D — Interview readiness | 21 | CPU demo/chaos, KubeRay/GPU design, Python coding mock, system design and DataRobot/OpenVPN stories |

**P0** = required skill evidence; **P1** = production realism; **P2** = optional advanced experiments. A *real* GPU cluster is **not** a prerequisite for the CPU MVP. GPU design must still be accurately understood, and live GPU performance claims require an actual GPU run.

The core demo can use HTTP directly; SDK and live P1 tenant/metrics demonstrations are optional. Required KubeRay/GPU design (`MLP-015–016`, design-only allowed) remains part of Gate D. If P1 scope is deferred, leave full Gate C incomplete; Gate D may still pass. See the [57–81.5-hour task estimate and bootstrap allowance](03-planning/README.md#estimates-and-availability) before treating the target days as dates.

## Implementation constraints

- This documentation bundle does **not** modify your application code, Python packages, repo name, CI, or infrastructure.
- `MLP-001` … `MLP-026` IDs stay stable to match the original starter exercises and earlier backlog.
- Only call features complete after running tests or recording explicit architecture-only evidence.
- Do not claim that Ray, Flyte, DDP, GPU checkpointing or any specific vendor tool was used at DataRobot without historical confirmation.

See also [work protocol](06-engineering/working-agreements.md) and [Definition of Done](06-engineering/definition-of-done.md).
