# Observability, reliability objectives and cost

## Signals

| Layer | Metric / log | Why |
|---|---|---|
| API | request count, error rate, p50/p95 latency | Admission and availability |
| Queue | depth, age of oldest queued job, queue wait | Capacity and fairness |
| Controller | lease age, reconciliation count, dispatch retry | Durable orchestration health |
| Training | attempts, retries, failures, duration, checkpoint age | ML reliability and recovery |
| Inference | request throughput, p95 latency, 4xx/5xx, model version | Serving SLO and rollout |
| Resources | allocated CPU/GPU time, GPU usage (if real GPU) | Capacity and waste |
| Tenants | active jobs / quota, queued jobs by tenant | Isolation and starvation |

Use bounded-cardinality metric labels (`tenant_tier`, `model_family`, `status`), not unbounded `job_id` labels. High-cardinality identifiers belong in structured logs/traces.

## Cost estimate

For a simplified learning exercise:

`estimated_gpu_cost = allocated_gpu_count * allocated_hours * configured_hourly_rate`

Track queue time separately from billed allocation time. If CPU-only or simulated, display **estimated/simulated** cost. This is not a provider billing report.

## SLO discussion

Separate training queueing/availability SLOs from inference latency/error SLOs. Training may be batch-oriented and retryable; inference may be latency-sensitive and requires warm capacity or defined cold-start behavior.

## Experiments

- Submit bursts of jobs and record API acknowledgement vs actual queue latency.
- Kill a worker and count recovery attempts / lost work since checkpoint.
- Increase inference concurrency, inspect p95 latency and replica changes.
- Add quota denial, check that it is reported and does not starve other tenants.
