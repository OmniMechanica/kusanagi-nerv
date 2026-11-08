# Security and multi-tenancy

## Threat model (learning scope)

**Assets:** job metadata, training data references, model artifacts, checkpoints, secrets, quota/capacity and billing/cost information.  
**Actors:** authenticated ML engineer, platform operator, execution worker and a malicious cross-tenant caller.

## Design requirements

1. Resolve tenant ownership from a trusted authentication principal, never from arbitrary user input in production.
2. Apply tenant filtering to list/get/cancel/deploy/predict and artifact retrieval.
3. Limit concurrent jobs and GPU allocations by tenant; explicitly define behavior for waiting jobs.
4. Use short-lived/workload identity for storage access where possible; never commit credentials or tokens.
5. Do not allow arbitrary command execution from untrusted training specs in the MVP.
6. Protect sensitive training metadata and artifacts in logs.
7. Treat user-controlled model content as potentially unsafe when choosing serialization/loading formats; avoid untrusted pickle payloads.
8. Set boundaries for shared Ray clusters: process isolation alone is not a hard security boundary.

## Proof cases

- Tenant A cannot list or access Tenant B's job, model or artifact.
- Tenant A cannot cancel Tenant B's work.
- Tenant quota exceedance yields a deterministic and documented response.
- An unauthenticated request is rejected after the auth milestone.
- Security decisions are documented in a short ADR/threat note.

## Deferred for production

mTLS, KMS, artifact encryption/key management, workload network policy, hard execution sandboxing, supply-chain scanning, compliance audits and hardened isolation of arbitrary tenant code. Flag these as unresolved security work, not completed production controls.
