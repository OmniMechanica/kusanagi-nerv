# Reliability and recovery design

## Failure matrix

| Failure | Expected response | Proof |
|---|---|---|
| API restarts after commit | Job and idempotency preserved | Restart / repeat request test |
| Controller dies before dispatch | Lease expires; another controller claims | Restart controller |
| Controller dies after Ray submit | Reconcile by submission ID; prevent unbounded duplicates | Crash injection between submit/commit |
| Ray worker dies mid-training | Retry policy and checkpoint-based resume | Kill worker; confirm resumed epoch |
| Node temporarily unavailable | Job waits/retries within policy; terminal failure after limit | Simulated/controlled node failure |
| Checkpoint/object store unavailable | Surface retryable error and preserve last valid artifact | Temporary MinIO outage |
| Cancel races with success | Correct terminal outcome, no false cancellation | Deterministic race test |
| Submit accepted, response lost, then cancel | Keep cancellation intent; look up/stop the same submission identity | Delay submit response and cancel before first running observation |
| Poll misses the entire run | Commit observed success/failure from `QUEUED`, with validated result | Executor completes between polls |
| Requested GPUs unavailable | Visible queue reason; no unapproved CPU substitution | Scheduling condition test |
| Tenant crosses boundary | Denied access, no artifact leakage | Tenant isolation tests |

## Checkpoint content

At minimum: model `state_dict`, optimizer state, epoch/step, hyperparameters, deterministic seed and artifact version. Where relevant include scheduler, scaler and RNG state. Ensure checkpoint files are written atomically or with final manifest/marker, not mistaken for complete during partial upload.

## Retry ownership

For the MVP, the platform controller owns the attempt budget. `max_retries = N` permits at most `1 + N` ledger attempts. A new attempt is created only after the previous one is known to have ended or been rejected; reconcile network timeouts and unknown remote outcomes within the existing attempt.

Configure Ray Train worker recovery explicitly with `FailureConfig(max_failures=0)` for the controller-owned recovery experiment. Do not wrap training in another automatically retrying Ray task/actor without explicitly disabling or accounting for those retries. Worker failure must surface to the controller, which records failure and launches a new attempt from the last fully published checkpoint. Future internal worker recovery needs a separate documented budget and must not be counted as a new controller attempt implicitly.

Persist the checkpoint selected for the new attempt as `resume_checkpoint_uri` and pass it to the driver explicitly. Use an attempt-specific output path/run name, load model/optimizer/epoch from the selected checkpoint, and publish subsequent checkpoints under the new attempt. Do not rely on a new run automatically discovering the previous run's state. This also keeps writes from different attempts separate.

Ray Train distinguishes worker recovery from driver restart; see [its fault tolerance guide](https://docs.ray.io/en/latest/train/user-guides/fault-tolerance.html). Pin the actual Ray version before implementation and verify its checkpoint loading path. Cluster/head loss and live GPU failures remain outside the CPU MVP recovery claim.

## Recovery experiment

1. Start training for more epochs than can finish instantly.
2. Observe checkpoint at epoch N; record its URI and checksum/version.
3. Kill the *worker process*, not only the API. Ensure that the controller observes a failure.
4. Start retry/resumption from the saved checkpoint (same logical job, new attempt).
5. Confirm training continues after N and final metadata reflects recovery.
6. Record what you did **not** validate (e.g. real GPU node failure, global Ray head failure, network partition).

## Semantics to explain in interviews

- Retry-from-zero != checkpoint resume.
- Logical job != individual attempt.
- HTTP timeout != job cancellation.
- At-least-once dispatch != exactly-once execution.
- Controller liveness != worker liveness.
- A `CANCELLING` state indicates intent, not successful termination.
