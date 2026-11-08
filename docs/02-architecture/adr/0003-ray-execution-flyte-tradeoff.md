# ADR-0003: Ray as execution runtime; Flyte as comparison/optional PoC

**Status:** Accepted for capstone. **Date:** 2026-11-08.

## Context

The interview target values Ray/Flyte, distributed training and model serving. Implementing two orchestration stacks within 2–3 weeks would dilute depth.

## Decision

Use Ray Core, Ray Train and Ray Serve for execution/training/inference. Analyze Flyte's workflow DAGs, caching and retries in a comparison note; implement a Flyte proof of concept only as P2.

## Alternatives

Flyte-first orchestration, plain Kubernetes Jobs, Kubeflow Pipelines, managed SageMaker. All viable for different constraints.

## Consequences

We gain deeper Ray practice but must acknowledge tradeoffs: workflow orchestration and artifact-driven DAG lineage are not the same as Ray's distributed execution model. They can complement each other in larger systems.
