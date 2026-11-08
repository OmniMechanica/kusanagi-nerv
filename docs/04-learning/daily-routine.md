# Study-to-build cadence

## Default daily timebox (about 3 hours)

1. **50–60 min**: specific course sections / official documentation supporting today's ticket.
2. **90–110 min**: implement and test the smallest vertical feature slice.
3. **20–30 min**: explain code without help, review one failure mode, and note interview-ready learnings.

Use a **read → build → break → explain** loop. If video work is over budget, stop watching and implement. A 2–3 week project is not compatible with completing four long courses end-to-end plus platform development.

## Weekly focus

- **Week 1 (9–15 Nov):** Python async/backend, persistence, correctness, Ray Core.
- **Week 2 (16–22 Nov):** PyTorch, Ray Train/Serve, checkpoint recovery, CPU MVP.
- **Week 3 (23–29 Nov):** GPU/KubeRay, quotas, observability, security, demos and full mock interviews.

## Coaching rules

- Write the first implementation yourself, especially P0 Python exercises; then use AI for code review and adversarial test ideas.
- Every day capture one production failure scenario and a 2–3 minute answer.
- Distinguish current capstone code from historical DataRobot production experience.
- When stuck over 45 minutes, document the blocker and ask for a targeted design review instead of replacing the implementation with unreviewed generated code.
- Finish day 14 with a runnable CPU demonstration even if optional features are deferred.

## If only 14 days remain

Prioritize 001–011 + 013 + 014. Implement basic monitoring only if easy; defer real Kubernetes/GPU setup, polished docs and advanced Flyte PoC. Allocate at least one mock each for Python and platform design.
