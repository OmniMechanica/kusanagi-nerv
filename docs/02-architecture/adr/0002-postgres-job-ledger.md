# ADR-0002: PostgreSQL as the durable job ledger

**Status:** Accepted for capstone. **Date:** 2026-11-08.

## Context

The platform needs persistent jobs, attempts, idempotency and coordination across controller/API restarts without building a queueing product from scratch.

## Decision

Use PostgreSQL as source of truth and a separately running controller that safely claims queued rows using leases/transactions. Use unique submission identities for Ray reconciliation.

## Alternatives

Redis lists/streams, Kafka, managed cloud queues. They can be appropriate under higher scale, fan-out or different failure boundaries, but add operating surface for a timeboxed project.

## Consequences

DB load and queue scalability must be monitored. Protect against duplicate dispatch and controller crashes; do not claim exactly-once side effects. Revisit the design once throughput/coupling requirements justify a broker.
