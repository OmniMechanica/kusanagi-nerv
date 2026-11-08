# Evidence-backed experience stories (STAR templates)

**Goal:** prepare concise narratives showing your personal contribution. Fill the blanks using true details from your own experience; do not fabricate scale, SLO improvements, framework names or dates.

## Story A — DataRobot: GPU-backed ML platform in production

- **Situation:** What ML training/inference workflows existed? Which users and constraints?
- **Task:** What specific production ownership did you have in Python, ML orchestration and Kubernetes?
- **Action:** Which code or supporting infrastructure did *you* develop? How were failures detected/retried/recovered? What alternatives did you assess?
- **Result:** Measured impact if available; otherwise describe observable production outcome without numbers.
- **Tradeoff:** Why this orchestration/recovery method? What would you change today?
- **Do not infer:** Ray/Flyte/DDP, GPU scheduling ownership or checkpoint persistence technology unless you can verify them.

## Story B — DataRobot: difficult failure and repair

- **Situation:** Worker/node/queue failure type, blast radius.
- **Task:** Restore pipeline or prevent repeat outages.
- **Action:** Diagnosis, Python/Kubernetes fix, test/observability improvement.
- **Result:** Observed recovery and preventive action.
- **Follow-up:** Distinguish retry-from-zero from checkpoint restore.

## Story C — OpenVPN: Internal Developer Platform at scale

- **Situation:** Cloud/hybrid Kubernetes infrastructure; clusters at 1000+ nodes as documented in CV.
- **Task:** Reduce developer friction and operate reliable platforms.
- **Action:** Developer self-service, one-click deployments, automation, monitoring, architecture/leadership.
- **Result:** Proven reliability/productivity outcomes; add exact metrics only if supported.
- **Boundary:** GPU at OpenVPN was an infrastructure resource you consumed, not necessarily a GPU platform you owned.

## Story D — Capgemini: leadership and engineering tradeoffs

- **Situation:** Technical Lead, approximately 10-person team, 200+ microservices GitOps program as documented.
- **Task:** Standardization, coordination, architecture and mentoring.
- **Action:** Specific technical leadership choices and disagreements resolved.
- **Result:** Delivery/quality improvements with defensible evidence.

## Story E — A mistake or incident you owned

Pick a real event, avoid blame, explain: signal → hypothesis → debug → recovery → long-term prevention → personal learning.

## Rehearsal

Have **90-second**, **3-minute** and **8-minute** versions of each. Use numbers only when you can defend their source. Prefer what you built and operated over tool inventories.
