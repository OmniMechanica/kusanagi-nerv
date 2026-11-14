# Planning hub

## Source of truth

- [Priorities and critical path](priorities.md)
- [21-day roadmap with milestones](roadmap.md)
- [Machine-readable backlog](backlog.csv)
- [Progress and recorded evidence](progress.md)
- [Task specs: MLP-001 through MLP-026](tasks/)

**Ticket prefixes:** `MLP-` is retained from the original capstone scaffold to preserve IDs and links. The public project/product name is **KUSANAGI NERV**.

## Workflow

`Planned → In progress → In review → Done` (or `Blocked`). Mark Done only after acceptance criteria, test output, evidence label and at least one explanation/drill are complete.

One ticket per vertical slice. Prefer an end-to-end working increment to adding libraries that are not used. If a task exceeds its timebox, split implementation from design, record the deferred scope and preserve the critical path.

Start with [repository bootstrap](../06-engineering/bootstrap.md) as the first step of `MLP-001`; it is not a separate ticket. MLP-001–006 are integrated into main through merged PRs; learning checkpoints remain pending. MLP-007 is In review with a real Ray Jobs controller adapter and restart/cancellation evidence. MLP-008 onward remain Planned. See [progress](progress.md) for evidence and the remaining learning checks.

## Critical path

- Backend: `001 → 002 → 003 → 004 → 005`; `007` joins `004 + 005 + 006`.
- Ray/training: `001 → 006 → 008 → 009`; `009` also depends on `006`.
- Recovery/serving: `007 + 009 → 010`; `008 + 010 → 011`.
- CPU proof: `007 + 010 + 011 → 013 → 014`.
- GPU design: `013 → 015 → 016` (design-only evidence is allowed).
- CPU chaos: `007 + 010 + 013 → 020`; final rehearsal joins `014 + 016 + 020 → 021`.

P1 tasks `012` and `017–019` are optional extensions. They do not block the CPU chaos drill or final rehearsal. Full Gate C remains incomplete if its P1 criteria are deferred; Gate D can proceed once its own P0 criteria pass.

## What is realistic in 14 days?

Complete tickets **MLP-001–011, 013, 014** first; `MLP-012` (SDK) is P1. Carry GPU/Kubernetes as a short **design-only** proof if running out of time. Do not spend days debugging a local GPU driver or building a full UI while core recovery is unproven.

## Estimates and availability

Available time at **2–3 hours/day** is **42–63 hours over three weeks**. The current estimates for scheduled tasks `MLP-001–021` total **57–81.5 hours**, plus **0.5–1 hour** for the bootstrap pre-step: **57.5–82.5 hours** before contingency. These are learning/build estimates; do not add the daily learning block a second time.

The full schedule therefore does not fit every availability scenario. Keep a reserve for integration failures and review, defer P1 scope when necessary, and replan weekly against actual remaining hours. Days and the 9–29 November window are target slices, not guaranteed completion dates. Study selected course sections only; completing four entire courses is outside this budget.
