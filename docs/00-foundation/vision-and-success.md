# Project vision, scope and success

## Brand

**KUSANAGI NERV** — **草薙・迷走神経**  
*The Wandering Nerve of Distributed Intelligence*

## Problem

Data scientists should submit training and inference workloads without configuring Kubernetes Pods, Ray clusters, GPUs, or storage by hand. The platform needs to manage execution, status, artifacts, retries, cancellation, recovery and resource access through a predictable self-service interface.

## Product statement

**A custom self-service ML training and inference platform that provides an HTTP API and an optional typed Python SDK, separates orchestration from execution, and demonstrates distributed compute, reliable training recovery and scalable model serving.**

## Target user

A Data Scientist or ML Engineer who understands models but should not need to be an expert in cluster provisioning. Secondary users: Platform Engineers who operate the cluster and track quotas, latency, reliability and GPU cost.

## Interviews and learning goals

The capstone exists to demonstrate (1) Python backend depth, (2) ML lifecycle and orchestration, (3) production distributed-systems thinking, (4) GPU/Kubernetes platform literacy, and (5) clear technical communication.

## Success at target Day 14 (non-negotiable core)

- A typed `POST /v1/jobs` accepts a training request and returns quickly, with durable state and idempotency.
- A separate controller dispatches jobs into Ray; training never blocks HTTP handlers.
- PyTorch trains a small, reproducible model; Ray Train demonstrates distributed execution on CPU.
- Model artifacts, training metadata and checkpoint/optimizer/epoch state are persisted in MinIO or S3.
- A controlled failure is recoverable **from a saved checkpoint**, not only by restarting at epoch zero.
- A successful model is deployed through the platform API and responds to inference requests via Ray Serve; the SDK is optional.
- One integration path `submit -> run -> checkpoint -> restore -> serve -> predict` has real automated tests.
- At least one Python coding mock and one system design mock are completed.

## Success at target Day 21 (required interview evidence)

- KubeRay deployment resources and GPU scheduling manifests/configuration are reviewed and validated; an actual GPU run is optional.
- A failure drill records the outcome, limitations and follow-ups.
- Final 10-minute CPU demo, architecture decisions, GPU/system-design discussion and interview scorecard are complete.

Optional P1 production hardening adds tested tenant isolation/quotas, metrics and GPU allocation cost **estimates**, expanded integration CI and the full ADR/security review. These complete **full Gate C**. If deferred, record the gaps and leave Gate C incomplete; required interview evidence and **Gate D** can still pass. Design-only discussions must not be presented as tested tenant or metrics behavior.

Days are planning targets. Replan against the [available hours and task estimates](../03-planning/README.md#estimates-and-availability), preserving checkpoint recovery, CPU demo and required GPU design before optional hardening.

## Non-goals

Production-grade multi-region HA; a real cloud billing engine; a feature-complete model registry; arbitrary user-provided containers; a full Kubernetes scheduler implementation; distributed ML research; guaranteed zero cold-start latency; managed secrets and compliance certification. Document these as future directions.

## Principles

1. **Start small, prove behavior.** Progress from pure domain code to external systems.
2. **Correctness before sophistication.** Tests and failure semantics precede autoscaling dashboards.
3. **Keep the ML model trivial.** Focus on platform engineering, not prediction accuracy.
4. **No feature theatre.** Mark CPU-validated, GPU-validated, simulated and design-only evidence distinctly.
5. **Be able to defend tradeoffs.** Every subsystem exists for a stated reason.
6. **Interview readiness is the outcome; feature count is not.**
