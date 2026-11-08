# Final project demo + chaos drill

## 10-minute walkthrough

1. Show architecture in `docs/02-architecture/overview.md` and 60-second role of each component.
2. Submit a training task through HTTP (for example, curl) or the optional Python SDK. HTTP returns immediately with job ID.
3. Show queued/running state, separate job controller, Ray Dashboard and job attempts.
4. Show latest checkpoint in MinIO/object store, including optimizer/epoch.
5. Kill a worker/process intentionally; allow retry and verify training resumes after saved epoch.
6. Deploy the successful model artifact through the platform API and show its Ray Serve inference endpoint.
7. Send simultaneous inference requests, show latency/replica behavior.
8. Present one GPU resource-request manifest (and real GPU execution if available).
9. If completed and tested, show P1 tenant quota denial and queue/cost metrics; otherwise state the deferred scope or discuss it as design-only.
10. Explain one thing that would break at production scale and a clear next architecture step.

The core final demo requires the CPU path and KubeRay/GPU design evidence (`MLP-015–016`). SDK and P1 live demonstrations are optional. Full Gate C remains incomplete if its P1 criteria are deferred; this does not block Gate D.

## Failure scenarios

- Controller crash during dispatch: lease expiry/reconciliation must avoid uncontrolled duplicate execution.
- Worker failure after checkpoint: no total restart from epoch 0.
- S3 temporary unavailable: retryable vs fatal errors, no false success.
- Cancellation while job is completing: no incorrect claim of guaranteed stop.
- Optional after `MLP-017`: two tenants use same ID / access protected routes; tenant boundaries hold. Without implementation, discuss as design-only and record the gap.
- GPU request when no GPU available: job remains pending/queued with visible reason, not silently converted to CPU.

## Quantitative notes to capture

- API submit p50/p95 (tiny local sample, don't oversell).
- Queue wait p50/p95; training duration.
- Job retries/failures/cancellations.
- Successful checkpoint resumes / total tested.
- Inference p95 throughput/latency under configured concurrency.
- If `MLP-018` is implemented: allocated GPU-hours and cost *estimate* from configured rate; otherwise present only a labelled design example.

**Evidence labels:** mark results as local CPU / real GPU / simulated GPU / architecture-only. Never describe an unrun test as measured.
