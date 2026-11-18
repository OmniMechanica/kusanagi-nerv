# Run the public CPU training and inference pipeline

MLP-013 runs two scenarios against FastAPI, PostgreSQL, SeaweedFS S3, Ray Train
and Ray Serve. Both use the public job, model and prediction endpoints.

## Run both scenarios

From the repository root with Docker and `uv` available:

```bash
scripts/test-cpu-pipeline.sh
```

On a Mac using Docker Desktop with another context selected:

```bash
KUSANAGI_DOCKER_CONTEXT=desktop-linux scripts/test-cpu-pipeline.sh
```

The script starts PostgreSQL and S3 with Compose and creates temporary Python
environments from `uv.lock`. It reads the published ports from Compose,
including ports configured in `.env`. Explicit `KUSANAGI_TEST_DATABASE_URL` and
`KUSANAGI_S3_ENDPOINT` values take precedence. On macOS it installs recovery and
serving in separate environments to keep PyArrow out of the Serve dashboard
subprocess.
Pytest starts two owned Ray clusters, the API and the controllers. Each case
migrates a disposable database, then removes that database and its own S3 job
prefix. Use `docker compose stop postgres s3` when finished with the shared
local services.

With existing environments and local services, export the settings in the
[checkpoint recovery guide](checkpoint-recovery.md) and `KUSANAGI_TEST_DATABASE_URL`
from the [PostgreSQL guide](postgres.md). On Linux install both extras and run:

```bash
uv run --locked --extra recovery --extra serving pytest -q -s tests/integration/test_cpu_pipeline.py
```

On macOS run pytest in the recovery environment with
`KUSANAGI_TEST_SERVING_PYTHON` set to the serving environment's `bin/python`.
See [model deployment](model-deployment.md) for the separate serving setup.

## Evidence

- Both scenarios receive `202` and a durable `Location` before the training
  controller starts. The restarted API reads the same job ID and creation time.
- The baseline completes one two-worker CPU DDP attempt and publishes its final
  checkpoint and model. The worker-loss scenario commits the epoch-2 checkpoint
  before rank zero receives `SIGKILL`. The controller submits one retry with a
  different ID. The retry restores Adam step 12 and starts at epoch 3.
- The client reads `result_model_id`, requests deployment, polls `ready` and
  invokes `predict`. Four returned values match inference on the exact registered
  artifact and the equivalent uninterrupted CPU training run within tolerance.

Each successful case prints a JSON line prefixed with `CPU_PIPELINE` containing
the attempt IDs, selected recovery checkpoint when applicable, artifact URI,
model ID and version, deployment identity, and prediction response. The sample
S3 keys are scoped to the disposable run. See [recorded results](../03-planning/progress.md).
