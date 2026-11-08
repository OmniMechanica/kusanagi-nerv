# Documentation change log

## 2026-11-08 — v0.3 Vendor-neutral interview guidance

- Made the target role, interview scorecard and scaling prompts vendor-neutral; removed the company-specific vacancy link.

## 2026-11-08 — v0.2 Contract review and implementation readiness

- Kept the eight job states and added queued cancellation, pre-running failure/retry and missed-running-observation success paths. Distinguished pure graph checks from persistent execution guards; MLP-001 now covers all 64 state pairs.
- Defined a canonical CreateJob schema without tenant input, explicit cancellation responses, result model ID/version, model readiness and the P0 self-service deploy/predict flow.
- Selected Ray Jobs for the durable adapter, with submission identity persisted before dispatch, explicit unknown/not-found semantics and controller-owned retry budget/checkpoint selection.
- Added active execution reservation semantics and the cross-controller tenant-cap race case for MLP-017.
- Moved server-side model registration/deployment into MLP-011; MLP-012 now wraps the completed API as an optional SDK.
- Corrected recovery/chaos/final-rehearsal dependencies, kept GPU design on the final P0 path, and separated optional full Gate C from Gate D. Synchronized task cards, CSV, indexes and demo guidance.
- Added a bootstrap pre-step inside MLP-001 for the actual empty application repository, with a Python/package/tooling baseline and early lint/unit CI. No new ticket IDs were added.
- Documented the 57–81.5-hour task budget plus 0.5–1-hour bootstrap allowance and the need to replan against available hours.
- Documentation changes only. All 26 implementation tickets remain Planned; no application code, environment setup, runtime tests or CI execution is claimed by this revision.

## 2026-11-08 — v0.1 Planning baseline

- Renamed the capstone product to **KUSANAGI NERV — 草薙・迷走神経**, tagline *The Wandering Nerve of Distributed Intelligence*.
- Split the original 21-day plan and 26 tickets into navigable sections.
- Mapped four CourseHunter sources and official technical references to engineering tasks.
- Defined explicit P0/P1/P2 priorities, verification gates and evidence standards.
- Added API/state contracts, architecture, reliability/GPU/tenancy design and decision records.
- Introduced individual tasks with dependencies, implementation steps, acceptance checks and drills.
- Documentation only; **no local repository implementation has been modified**.

Follow-up changes should record affected tasks, scope changes and decisions.
