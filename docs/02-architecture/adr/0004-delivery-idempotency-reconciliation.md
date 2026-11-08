# ADR-0004: At-least-once dispatch with idempotent reconciliation

**Status:** Accepted for capstone. **Date:** 2026-11-08.

## Context

A controller can crash after submitting work remotely but before persisting an execution ID. API retries and duplicate controller actions are inevitable.

## Decision

Persist logical jobs and attempt identities, use request idempotency keys and deterministic submission tokens, and reconcile observed remote execution state after controller restart.

The MLP-007 adapter uses Ray Jobs with a `submission_id` derived from job ID and attempt number, persisted before dispatch and accepted by submit/status/cancel. Controller restarts reuse that identity on the recorded cluster. An unknown outcome does not create a new attempt. `QUEUED` includes remote pending work; cancellation after submission intent uses `CANCELLING` until safe completion is observed.

The controller owns the MVP retry budget. Disable internal training recovery for the cross-attempt recovery test, select a fully published checkpoint explicitly, and persist a new attempt only after confirmed failure. Guard writes with current attempt/lease authority; model registration is idempotent per successful logical job. Detailed contracts live in [lifecycle](../../01-product/job-lifecycle.md), [executor](../components.md#contract-between-control-and-execution) and [reliability](../reliability.md#retry-ownership).

## Alternatives

Best-effort fire-and-forget dispatch (unreliable); theoretical exactly-once assumption (invalid across independent systems); distributed transactional execution (too complex for target scope).

## Consequences

Some duplicate execution windows can remain in abnormal failures. We must detect/limit them, isolate their effects, and explain what can be guaranteed.
