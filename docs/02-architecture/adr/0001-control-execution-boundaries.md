# ADR-0001: Separate the control plane from the execution plane

**Status:** Accepted for capstone. **Date:** 2026-11-08.

## Context

Training can last minutes or hours and requires retries, status tracking and remote resource allocation. FastAPI process lifetime and HTTP timeouts must not be tied to training execution.

## Decision

Use a thin FastAPI **control plane** for validation, authorization, durable submission and status. Run a separate **controller/reconciler** process that dispatches and monitors jobs. Use Ray as the **execution plane** for tasks, training and model serving; store durable state in PostgreSQL and artifacts in object storage.

## Alternatives

- Run training synchronously inside HTTP handlers (blocks and couples lifecycles).
- Use FastAPI `BackgroundTasks` as the execution queue (not durable across crashes).
- Use only Kubernetes Jobs or a broker with independent workers (viable but less direct Ray Train/Serve learning).

## Consequences

The system needs leases, attempt identities, reconciliation, cancellation semantics and monitoring across process boundaries. This increases complexity but is necessary for realistic production reliability and credible interview demonstrations.
