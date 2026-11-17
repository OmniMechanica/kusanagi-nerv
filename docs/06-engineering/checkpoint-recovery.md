# Recover a training attempt from S3

MLP-010 connects the Ray Jobs controller to the two-worker CPU DDP workload.
A completed epoch publishes model, Adam, minibatch RNG, epoch, history and
compatibility metadata to an S3-compatible object store. The controller chooses
and persists a specific checkpoint before dispatching a new attempt.

## Run the local experiment

The Compose service uses SeaweedFS 4.53 with the S3 API, pinned by image digest.
It exposes loopback port 58333 and retains data in a named volume. A compatible
local MinIO endpoint can be supplied with the same settings. The current client
expects a pre-created bucket and explicit local credentials. The values below
are public development defaults.

```bash
uv sync --locked --extra recovery
docker compose up -d --wait postgres s3
export KUSANAGI_DATABASE_URL='postgresql+asyncpg://kusanagi:local-development-only@127.0.0.1:55432/kusanagi'
export KUSANAGI_S3_ENDPOINT='http://127.0.0.1:58333'
export KUSANAGI_S3_BUCKET=kusanagi-training
export KUSANAGI_S3_ACCESS_KEY=kusanagi-local
export KUSANAGI_S3_SECRET_KEY=local-development-only
uv run --locked --extra recovery alembic upgrade head
```

Start the cluster and controller in separate terminals with the same S3
variables exported. The Ray driver and workers inherit the S3 settings from
the cluster process. The controller sends the non-secret object store identity
and selected checkpoint in the job payload. Credential values stay outside
the Ray Jobs request and the PostgreSQL ledger.

```bash
# Ray terminal
uv run --locked --extra recovery python -m kusanagi_nerv.runtime.local \
  --info-file /tmp/kusanagi-recovery-ray.json --cpus 2 --port 8265

# Controller terminal, with KUSANAGI_DATABASE_URL and KUSANAGI_S3_* exported
uv run --locked --extra recovery python -m kusanagi_nerv.controller \
  --executor ray-training --ray-info-file /tmp/kusanagi-recovery-ray.json \
  --experiment-kill-after-epoch 2
```

Start the [API](postgres.md#start-the-local-database-and-api) in another terminal.
Use `--extra recovery` on its `uv run` command when sharing the same environment.
Then submit the synthetic CPU job and poll the response's `Location`.

```bash
curl -i http://127.0.0.1:8000/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"model":"tiny-mlp","dataset":"synthetic-v1","epochs":5,"resources":{"cpus":2,"gpus":0,"memory_gib":0.25},"max_retries":1}'
```

This executor accepts `tiny-mlp`, `synthetic-v1`, exactly two CPUs, no GPUs and
1–200 epochs. The fault experiment needs a configured epoch below the final
epoch. `--experiment-kill-after-epoch` applies to the first attempt of jobs handled
by that controller. Use a dedicated development ledger for fault experiments.
Omit the flag for ordinary training. The driver reserves zero Ray CPUs, and each
training worker reserves one CPU. The requested memory is a logical entrypoint
reservation. Storage scope is local loopback on a single host.

## What persists across the failure

1. Rank zero serializes the full epoch checkpoint and writes it under
   `jobs/<job-id>/attempts/<submission-id>/checkpoints/<epoch>.pt`. A single
   conditional S3 `PUT` creates the object atomically. It prevents replacing
   an existing key, and SHA-256 metadata identifies the bytes. Identical replay
   is accepted after a read and byte comparison. Objects are bounded to 4 MiB.
2. In the experiment, rank zero sends itself `SIGKILL` after storage acknowledges
   epoch 2. Ray Train recovery is set to zero. The Ray Jobs driver uses exit code
   `75` only when the typed Ray error establishes worker actor death, including
   Ray 2.60's worker health-check wrapper. The controller waits for Ray Jobs
   `FAILED` before inspecting the failed attempt's checkpoint prefix. It verifies
   candidate metadata and bytes from newest to oldest and stores the selected
   reference in `retry_checkpoint`. A failed attempt that published no usable
   checkpoint can select its verified `resume_checkpoint` again. A retry requires
   available budget. Network uncertainty keeps the same submission identity and
   consumes no additional attempt.
3. The transaction that creates the successor copies the selected URI, SHA-256
   and epoch into `resume_checkpoint`. The adapter passes that exact reference to
   the new driver. Each rank reads and verifies the object before loading it with
   `weights_only=True`. Restore validates the job, source attempt, epoch, model,
   dataset, configuration, worker count, device and PyTorch version. It restores
   model weights, Adam state and the minibatch generator, then starts epoch 3.
4. Rank zero publishes new checkpoints under the successor's prefix. After
   `fit()` succeeds, the driver publishes final inference weights to that
   attempt's `model.pt` S3 key. The adapter downloads both the artifact and final
   checkpoint with the object size limit and verifies their SHA-256 before
   committing job success. `GET` exposes
   `checkpoint_uri` and `artifact_uri`; the attempt records retain the selected
   checkpoint, the checkpoint each attempt published, starting epoch and restored
   Adam step.

The checkpoint itself contains optimizer state for a same-configuration restart.
The final model artifact uses the MLP-008 inference format. Read a checkpoint via
the S3 API and verify SHA-256 before loading. The workflow retains Ray Jobs records
and relies on the previous attempt's objects remaining readable. A failure after
the last checkpoint and before artifact publication can resume from the previous
epoch if that checkpoint is available. A failed attempt is terminal when neither
its own nor its inherited checkpoint can be used, or its retry budget is
exhausted. Other confirmed failures are currently permanent.

No overwrite or list-by-global-latest operation crosses an attempt boundary.
Ownership of a PostgreSQL lease and the existing Ray Jobs dispatch/cancellation
fences still apply. An owned running or pending Ray Job can receive `stop` during
an S3 outage. The controller waits for Ray to report a terminal state. For this
CPU experiment the Boto3 client uses explicit loopback settings and bounded I/O.
Availability of a production S3 deployment,
cloud identities, distributed storage topology and lifecycle policy require
separate validation. [S3 PutObject conditionals](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)

## Verify the proof

```bash
export KUSANAGI_TEST_DATABASE_URL='postgresql+asyncpg://kusanagi:local-development-only@127.0.0.1:55432/postgres'
uv run --locked --extra recovery pytest -q tests/integration -m recovery
```

The tests lose the first submit response, then verify that the controller keeps
attempt one. A new Python process reads the epoch-2 checkpoint and sees Adam
step 12. With budget `1`, attempt two resumes from epoch 3, publishes an epoch-5
checkpoint and produces predictions that match an uninterrupted equivalent
batch within float tolerance. With budget `0`, Ray still fails after the worker
loss and PostgreSQL records one failed attempt with its checkpoint and no model.
With budget `2`, attempt two loses a worker immediately after restoring epoch 2,
before publishing a checkpoint. Its retry selection still points to attempt one's
checkpoint, and attempt three starts at epoch 3. The S3 tests check that matching
metadata cannot establish success for corrupted bytes and that an invalid digest
on the latest checkpoint leaves earlier valid checkpoints eligible for retry.
A Ray Jobs API stub verifies that cancellation sends `stop` during an S3 outage.
The tests use disposable databases and delete only their own job prefixes.

To inspect the ledger for a job, substitute its UUID below.

```sql
SELECT attempt_no, submission_id, failure_kind, published_checkpoint,
       resume_checkpoint, retry_checkpoint, execution_result
FROM job_attempts
WHERE job_id = '<job UUID>'
ORDER BY attempt_no;
```

Learning checkpoint: explain why a new attempt needs the Adam step and RNG
state as well as the weights, and what happens if a worker dies while uploading
a checkpoint.
