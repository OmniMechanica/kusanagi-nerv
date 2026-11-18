# Deploy a registered model with Ray Serve

The deployment controller reads requests from PostgreSQL and reconciles them
against the authenticated dashboard of the selected local Ray cluster. Each
model has a stable application name derived from its model ID. An explicit
retry of a failed deployment changes the revision and keeps the name.

## Start the local services

Follow the [S3 and PostgreSQL setup](checkpoint-recovery.md#run-the-local-experiment).
Apply `alembic upgrade head`, currently `0009_job_model_result`, before starting
the API or the controllers. Export `KUSANAGI_DATABASE_URL` for the API and
controllers and the `KUSANAGI_S3_*` settings for both the API and Ray clusters.

The commands below use a separate serving environment and a separate local Ray
cluster. On macOS this avoids a Ray 2.60 dashboard subprocess crash when its
Serve module imports PyArrow from the training environment. The serving cluster
and training cluster use the same PostgreSQL ledger and S3 bucket. Each model
replica reserves 0.25 CPU.

```bash
export UV_PROJECT_ENVIRONMENT=/tmp/kusanagi-serving-venv
uv sync --locked --extra serving
uv run --locked --extra serving python -m kusanagi_nerv.runtime.local \
  --info-file /tmp/kusanagi-serving-ray.json --port 8266
```

Run the deployment controller in its own terminal with the same serving
environment, Ray info file and database URL.

```bash
export UV_PROJECT_ENVIRONMENT=/tmp/kusanagi-serving-venv
uv run --locked --extra serving python -m kusanagi_nerv.controller.deployments \
  --ray-info-file /tmp/kusanagi-serving-ray.json --serve-port 8001
```

The controller uses the dashboard token and checks the Ray session before
applying the complete application configuration. Use one ledger for the Serve
applications on this local cluster. If the dashboard reports an application
from outside that ledger, the controller leaves the configuration in place.
The Serve HTTP proxy binds to loopback. If the authenticated dashboard reports
an incompatible listener or a disabled proxy, the controller reports affected
deployments as `deploying` until it observes a compatible configuration.

## Request deployment and check status

Use `result_model_id` from a successful training job. Start the FastAPI service
using the serving environment and the same PostgreSQL/S3 settings. Set
`KUSANAGI_SERVE_PORT` in the API environment to the deployment controller's
`--serve-port` value. Both default to 8001. On API port 8000, submit and then check
the model at the returned `Location`.

```bash
curl -i -X POST "http://127.0.0.1:8000/v1/models/$model_id/deploy"
curl -i "http://127.0.0.1:8000/v1/models/$model_id"
```

The first request downloads the artifact with the object size limit, verifies
SHA-256 and checks that it loads as a model with finite weights. A Serve replica
repeats those checks when it starts. `202` records a durable request. Another
request while deploying returns the same identity. `200` means the controller
has observed the matching application running. A restarted controller finds the existing application by
name and revision, even if the first controller stopped after Ray accepted it.
The controller records confirmed application states even when the next
configuration update fails. If Ray reports failure, the controller commits
`failed` and `deploy_error` before removing the app from its desired config.
A new deploy request starts another revision with the same application name.

## Predict through the public API

When the model reports `ready`, send a batch of 1–256 rows with four finite
float32-range features per row.

```bash
curl -i "http://127.0.0.1:8000/v1/models/$model_id/predict" \
  -H 'Content-Type: application/json' \
  -d '{"instances":[[0.5,-2,0.25,0],[1,2,3,4]]}'
```

The response contains `model_id`, `version` and a prediction per row in order.
The API checks tenant access and the ready deployment before forwarding to the
loopback proxy. The replica evaluates its loaded CPU model in inference mode.
It recomputes rows with a non-finite float32 result using float64.
The API validates replica identity, artifact digest and deployment revision
before returning predictions.

For the CPU demo, record p95 latency at the public `predict` endpoint with the
batch size and concurrency. Measure cold deployment readiness separately from
warm inference. Track `model_not_ready` and `serving_unavailable` as separate
outcomes alongside successful requests.

## Verify locally

```bash
export UV_PROJECT_ENVIRONMENT=/tmp/kusanagi-serving-venv
uv run --locked --extra serving pytest -q tests/integration/test_model_deployments.py
```

The real test uses a CPU weights artifact in S3 and the public model endpoints.
After Ray accepts the application, the deployment controller exits. A fresh
controller observes the ready replica. The test then restarts the Serve service
and checks that concurrent predictions match local inference on the saved weights.
