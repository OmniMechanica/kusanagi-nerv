# Definition of Done

A ticket is **Done** only when all applicable checks pass:

- [ ] Functional behavior meets each acceptance criterion.
- [ ] Correct types, errors and valid/invalid state transitions are covered by tests.
- [ ] Unit tests pass; integration/E2E tests run where required by the ticket.
- [ ] Failure behavior or cancellation/timeout/retry behavior is documented and tested where applicable.
- [ ] Code is comprehensible, formatted and passes lint checks.
- [ ] Security/tenancy implications are understood.
- [ ] No credentials, PII or costly cloud credentials are committed.
- [ ] Architecture or API docs were updated if the behavior changed.
- [ ] Evidence is tagged `TESTED-CPU`, `TESTED-GPU`, `SIMULATED` or `DESIGN-ONLY` accurately.
- [ ] You can explain the design and at least one important tradeoff without reading code.
- [ ] Task state / next step updated in the backlog and progress journal.

## Gate DoD

- **Gate A:** Durable job ledger and controller produce reproducible, tested transitions/recovery behavior.
- **Gate B:** CPU E2E, real checkpoint resume and inference succeed with scripted commands.
- **Gate C (optional full extension):** GPU/KubeRay design plus tested P1 tenant controls, metrics/cost, expanded CI and reviewed ADRs. Leave this gate incomplete when its P1 work is deferred.
- **Gate D:** Complete CPU demo/failure drill, required KubeRay/GPU design and scored mock interviews with improvement notes. HTTP is sufficient; optional SDK/P1 work and full Gate C do not block this gate.

A well-reasoned design-only GPU manifest is valid *design evidence* but cannot be marked a successfully executed GPU run.
