# Architecture and decisions

- [System overview and Mermaid diagram](overview.md)
- [Component responsibilities and runtime boundaries](components.md)
- [PostgreSQL data model, leases and consistency](data-model.md)
- [Failure matrix and checkpoint recovery](reliability.md)
- [Kubernetes, GPU scheduling and resource isolation](gpu-kubernetes.md)
- [Security and multi-tenancy](security-and-tenancy.md)
- [Observability and cost](observability-and-cost.md)

## ADRs

- [ADR-0001 — Initial architectural boundaries](adr/0001-control-execution-boundaries.md)
- [ADR-0002 — PostgreSQL ledger](adr/0002-postgres-job-ledger.md)
- [ADR-0003 — Ray execution vs Flyte](adr/0003-ray-execution-flyte-tradeoff.md)
- [ADR-0004 — At-least-once and idempotent reconciliation](adr/0004-delivery-idempotency-reconciliation.md)
- [ADR-0005 — Object-store checkpoints](adr/0005-object-store-checkpoints.md)

New decisions should be recorded as numbered ADRs with **Context → Decision → Alternatives → Consequences** and a status. A conceptual diagram does not imply a deployed service.
