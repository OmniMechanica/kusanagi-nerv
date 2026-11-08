# Product and contracts

- [Functional requirements and SLO-oriented learning targets](requirements.md)
- [HTTP API contracts, validation and idempotency](api-contracts.md)
- [Job state machine, retries and cancellation](job-lifecycle.md)

**Design principle:** the API accepts work quickly; a durable controller + execution plane handle ML operations.
