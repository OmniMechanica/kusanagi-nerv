# Ray Train with two CPU workers

MLP-009 runs the MLP-008 regressor with Ray Train v2 and PyTorch
`DistributedDataParallel` (DDP). The CPU run is **TESTED-CPU**. The CUDA path
is **DESIGN-ONLY** for execution. The durable controller consumes training results and resumes from checkpoints
through the [MLP-010 recovery adapter](checkpoint-recovery.md).

## Run an attempt

```bash
uv sync --locked --extra ray-train
training_dir=$(mktemp -d)
uv run --locked --extra ray-train python -m kusanagi_nerv.training.ray_train \
  --submission-id ddp-demo-1 --device cpu --num-workers 2 \
  --seed 42 --epochs 20 --batch-size 32 --output "$training_dir/model.pt"
```

The CLI starts an authenticated local Ray runtime and removes it on exit.
`train_distributed(config, output)` runs on a Ray runtime initialized by its
caller, using a local filesystem visible to the driver and workers on one host.
Use a unique submission ID per attempt and a new output path. The extra pins
Ray Train 2.60.0 and reuses the CPU PyTorch wheel from MLP-008.

## What the ranks do

1. Ray reserves one logical CPU per worker and sets up the PyTorch process group
   with Gloo for CPU. Each worker has its own PID. World size is the total number
   of workers. World rank identifies a worker in the group, and local rank
   identifies it on its node.
2. Every rank generates the same synthetic train/evaluation split and the same
   epoch permutation. Rank `r` takes `permutation[r::world_size]`. With two
   workers, each sees 192 disjoint training rows and each performs six updates
   per epoch at a per-rank batch size of 32. Supported world sizes 2, 3 and 4
   divide the 384 rows evenly. The tail batch is equally sized across ranks.
3. DDP keeps one model replica per rank and reduces gradients during backward.
   Each optimizer applies the same averaged gradient. The effective batch for
   a full step is 64 examples in the default run. Throughput also depends on
   collective latency and bandwidth, and the slowest rank can hold up the group.
   This tiny workload measures correctness without making a speedup claim.
4. The ranks reduce training MSE over their equal-size shards. Rank 0 evaluates
   the bare model on the full held-out set and publishes the final weights.
   Its evaluation bypasses DDP forward collectives, so the other ranks can wait
   for the following training synchronization. The report includes per-rank
   parameter SHA-256 values to verify synchronized weights.

| Tool or strategy | Role |
|---|---|
| `torchrun` | Starts PyTorch workers and supplies rendezvous and rank settings. Ray Train supplies process orchestration and resource placement here. |
| DDP | Replicates parameters and optimizer state per rank and synchronizes gradients. |
| FSDP | Shards model state across ranks to reduce per-rank memory, gathering parameters as needed and reducing gradients into shards. |
| Gloo / NCCL | Collective communication backends. This CPU exercise uses Gloo. CUDA workers would use NCCL. |

Reference: [Ray Train with PyTorch](https://docs.ray.io/en/latest/train/getting-started-pytorch.html)
and [accelerator configuration](https://docs.ray.io/en/latest/train/user-guides/using-accelerators.html).

## Start and result contract

As part of the first epoch, every worker prepares its shard and enters a barrier
after DDP setup. Rank 0 then logs `KUSANAGI_TRAINING_STARTED ` followed by JSON
containing `submission_id`, `world_size`, `device`, `rank` and `worker_pid`.
Ray log forwarding may add a worker prefix or repeat the line. A consumer should
match the expected submission ID and handle repeat delivery idempotently. A
driver that is still waiting for worker placement cannot emit this event.

On success the driver prints `KUSANAGI_TRAINING_RESULT ` followed by JSON for
`DistributedResult`. It contains the attempt identity, driver PID, backend,
world size, rank evidence and a nested `TrainingResult` with metrics and the
absolute artifact path and SHA-256. The driver retrieves the rank-zero weights
from a temporary Ray Train checkpoint and publishes the file after `fit()`
succeeds. The file is loadable through the MLP-008 `load_model` API. This
temporary handoff contains inference weights and metadata. A resume checkpoint
with optimizer state and epoch lives in MLP-010.

Ray Train v2 uses `FailureConfig(max_failures=0, max_preemption_failures=0,
controller_failure_limit=0)`. Worker, preemption and Train-controller retry
budgets are zero. The platform controller owns subsequent attempt decisions.
See [Ray Train failure handling](https://docs.ray.io/en/latest/train/user-guides/fault-tolerance.html).

`--device cuda` requests one GPU and one CPU per worker with NCCL. It requires
a compatible CUDA PyTorch installation and provisioned GPU workers. A CPU-only
cluster cannot place that group, so it emits neither a training-start event nor
a success report. `RAY_TRAIN_WORKER_GROUP_START_TIMEOUT_S` bounds worker placement
wait in the pinned Ray version. GPU execution and performance require separate
hardware validation.

## Reproduce verification

```bash
uv run --locked --extra ray-train pytest -q tests/ray_train
```

The CPU case repeats the distributed run in separate processes and compares
rank identities, parameter digests, metrics and predictions. It also compares
predictions and aggregate training MSE to serial training with the equivalent
batch size of 64, allowing floating-point rounding differences. The pending
case starts a real Ray Train driver, requests GPU placement on a cluster with
zero GPUs and checks for the specific worker placement timeout. It also checks
the absence of the start event, success result and artifact. The startup cleanup
case delivers `SIGTERM` to the CLI after it creates the token and before it
imports Ray, then checks that the temporary directory was removed.

The observed default CPU run completed 20 epochs with Gloo. Driver PID was
3946, and worker PIDs were 4035 and 4034. Evaluation MSE went from `7.496293`
to `0.135955`, MAE was `0.233100`, and both worker parameter digests were
`8f32e34ea03d640d4d5ca0f525625b63d0defd415428988cf043985024717c0c`.
The result artifact hash for that particular attempt was
`7da57728ebdd33380086a9c471169110d2488e7c849117c436e49e9fa45f768d`.
File bytes include attempt identity and PIDs. Compare parameter hashes and
metrics across runs, and use the individual file hash for artifact integrity.

Learning checkpoint: explain why DDP needs matching step counts on all ranks,
how effective batch size changes with world size, and when FSDP or NCCL matters.
