"""Public train, recover, deploy and predict on two owned local Ray clusters."""

import asyncio
import importlib.util
import io
import json
import os
import socket
import sys
from pathlib import Path

import httpx2
import pytest

from kusanagi_nerv.executors.ray_jobs import RayJobsSettings
from kusanagi_nerv.training.distributed_config import START_PREFIX
from kusanagi_nerv.training.recovery_config import WORKER_LOST_EXIT
from tests.integration.test_api_restart import api_process
from tests.integration.test_model_deployments import deploy_process
from tests.integration.test_ray_jobs import controller_process, owned_ray_cluster
from tests.integration.test_training_recovery import (
    attempt_rows,
    read_checkpoint_in_fresh_process,
)
from tests.integration.test_training_recovery import object_store as object_store

pytestmark = [
    pytest.mark.integration,
    pytest.mark.recovery,
    pytest.mark.serving,
    pytest.mark.e2e,
]
EPOCHS = 3
CHECKPOINT_EPOCH = 2


@pytest.fixture(scope="module")
def serving_python():
    configured = os.environ.get("KUSANAGI_TEST_SERVING_PYTHON")
    if configured:
        path = Path(configured).expanduser()
        if not path.is_file() or not os.access(path, os.X_OK):
            pytest.fail(
                "KUSANAGI_TEST_SERVING_PYTHON must point to a serving interpreter"
            )
        # Keep the venv entrypoint path. Resolving its symlink would bypass it.
        return str(path.absolute())
    if sys.platform == "darwin" or importlib.util.find_spec("grpc_reflection") is None:
        pytest.skip(
            "Run scripts/test-cpu-pipeline.sh or configure the serving interpreter"
        )
    return sys.executable


@pytest.fixture(scope="module")
def training_cluster(tmp_path_factory, serving_python):
    if any(
        importlib.util.find_spec(name) is None for name in ("ray", "pyarrow", "torch")
    ):
        pytest.skip("Install the recovery extra in the test environment")
    # Always use an owned cluster, including when KUSANAGI_TEST_RAY_INFO is set.
    path = tmp_path_factory.mktemp("cpu-training") / "cluster.json"
    with owned_ray_cluster(path) as info:
        yield info


async def wait_for(client, location, predicate, *, process, timeout):
    view = {}
    try:
        async with asyncio.timeout(timeout):
            while True:
                assert process.poll() is None, (
                    f"Controller exited with {process.returncode}"
                )
                response = await client.get(location)
                assert response.status_code == 200, response.text
                view = response.json()
                if predicate(view):
                    return view
                await asyncio.sleep(0.1)
    except TimeoutError:
        pytest.fail(f"Timed out polling {location}. Last response: {view}")


async def ray_attempts(cluster, attempts):
    from kusanagi_nerv.executors.ray_training import events

    settings = RayJobsSettings.from_info_file(cluster)
    observations = []
    async with httpx2.AsyncClient(
        base_url=settings.address,
        headers={"Authorization": f"Bearer {settings.token}"},
        timeout=5,
        trust_env=False,
    ) as ray:
        for attempt in attempts:
            submission = attempt["submission_id"]
            status = await ray.get(f"/api/jobs/{submission}")
            status.raise_for_status()
            logs = await ray.get(f"/api/jobs/{submission}/logs")
            logs.raise_for_status()
            starts = events(logs.json()["logs"], START_PREFIX, submission)
            observations.append((status.json(), starts))
    return observations


@pytest.mark.parametrize("worker_loss", [False, True], ids=["normal", "worker-loss"])
async def test_public_cpu_pipeline(
    worker_loss,
    postgres_url,
    engine,
    object_store,
    training_cluster,
    serving_python,
    tmp_path,
    monkeypatch,
):
    import torch

    from kusanagi_nerv.training.config import TrainingConfig
    from kusanagi_nerv.training.loop import load_model, synthetic_split, train

    store, owned = object_store
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        serve_port = reserved.getsockname()[1]
    monkeypatch.setenv("KUSANAGI_SERVE_PORT", str(serve_port))

    # No training controller is running yet, so a successful response proves
    # that admission completed independently of job execution.
    with api_process(postgres_url) as (address, _):
        async with httpx2.AsyncClient(
            base_url=address, timeout=5, trust_env=False
        ) as first_api:
            response = await first_api.post(
                "/v1/jobs",
                json={
                    "model": "tiny-mlp",
                    "dataset": "synthetic-v1",
                    "epochs": EPOCHS,
                    "resources": {"cpus": 2, "gpus": 0, "memory_gib": 0.25},
                    "max_retries": int(worker_loss),
                },
            )
            assert response.status_code == 202, response.text
            accepted = response.json()
            assert accepted["status"] == "queued"
            owned.append(accepted["job_id"])
            job_path = response.headers["Location"]
            queued = (await first_api.get(job_path)).json()
            assert queued["status"] == "queued" and queued["attempt"] == 0

    with (
        controller_process(
            postgres_url,
            training_cluster,
            executor="ray-training",
            kill_after_epoch=CHECKPOINT_EPOCH if worker_loss else None,
        ) as trainer,
        api_process(postgres_url) as (address, _),
    ):
        async with httpx2.AsyncClient(
            base_url=address, timeout=5, trust_env=False
        ) as api:
            restarted = (await api.get(job_path)).json()
            assert restarted["job_id"] == accepted["job_id"]
            assert restarted["created_at"] == queued["created_at"]
            job = await wait_for(
                api,
                job_path,
                lambda view: view["status"] in {"succeeded", "failed", "cancelled"},
                process=trainer,
                timeout=120,
            )
            assert job["status"] == "succeeded", job
            attempts = await attempt_rows(engine)
            assert len(attempts) == job["attempt"] == 1 + int(worker_loss)
            final = attempts[-1]
            assert final["last_observation"] == "succeeded"
            assert final["published_checkpoint"]["epoch"] == EPOCHS
            assert job["checkpoint_uri"] == final["published_checkpoint"]["uri"]
            result = final["execution_result"]
            assert job["artifact_uri"] == result["artifact_uri"]
            runtime_attempts = await ray_attempts(training_cluster, attempts)
            first_epoch = CHECKPOINT_EPOCH + 1 if worker_loss else 1
            assert all(
                any(
                    event["rank"] == 0
                    and event["world_size"] == 2
                    and event["device"] == "cpu"
                    for event in starts
                )
                for _, starts in runtime_attempts
            )
            assert {event["first_epoch"] for event in runtime_attempts[-1][1]} == {
                first_epoch
            }
            checkpoint = None
            if worker_loss:
                first = attempts[0]
                assert first["last_observation"] == "failed"
                assert first["failure_kind"] == "transient"
                assert runtime_attempts[0][0]["driver_exit_code"] == WORKER_LOST_EXIT
                checkpoint = first["published_checkpoint"]
                assert checkpoint["epoch"] == CHECKPOINT_EPOCH
                assert (
                    final["resume_checkpoint"]
                    == first["retry_checkpoint"]
                    == checkpoint
                )
                restored = read_checkpoint_in_fresh_process(checkpoint)
                assert restored == {"epoch": 2, "steps": [12], "history": 2}
                assert result["restored_optimizer_step"] == 12
                assert first["submission_id"] != final["submission_id"]
            else:
                assert final["resume_checkpoint"] is None
                assert result["restored_optimizer_step"] == 0

            model_id = job["result_model_id"]
            assert model_id and job["result_model_version"] == "v1"
            model_path = f"/v1/models/{model_id}"
            deploy = await api.post(model_path + "/deploy", timeout=15)
            assert deploy.status_code == 202, deploy.text
            assert deploy.headers["Location"] == model_path

            serving_path = tmp_path / "serve.json"
            with (
                owned_ray_cluster(serving_path, python=serving_python),
                deploy_process(
                    postgres_url,
                    serving_path,
                    serve_port,
                ) as (deployer, log),
            ):
                try:
                    ready = await wait_for(
                        api,
                        model_path,
                        lambda view: view["deploy_state"] in {"ready", "failed"},
                        process=deployer,
                        timeout=65,
                    )
                except BaseException as exc:
                    log.seek(0)
                    exc.add_note(log.read()[-6000:])
                    raise
                assert ready["deploy_state"] == "ready", ready
                assert ready["artifact_uri"] == job["artifact_uri"]
                assert ready["artifact_sha256"] == result["checksum"]
                assert (
                    ready["deployment_identity"] == deploy.json()["deployment_identity"]
                )
                _, _, inputs, _ = synthetic_split()
                features = inputs[:4]
                reply = await api.post(
                    model_path + "/predict", json={"instances": features.tolist()}
                )
                assert reply.status_code == 200, reply.text
                output = reply.json()
                assert output["model_id"] == model_id
                assert output["version"] == job["result_model_version"]
                data = store.read(job["artifact_uri"], ready["artifact_sha256"])
                reference = train(
                    TrainingConfig(epochs=EPOCHS, batch_size=64),
                    tmp_path / "reference.pt",
                )
                with torch.inference_mode():
                    got = torch.tensor(output["predictions"], dtype=torch.float32)
                    torch.testing.assert_close(
                        got, load_model(io.BytesIO(data))(features)
                    )
                    torch.testing.assert_close(
                        got,
                        load_model(reference.artifact_path)(features),
                        rtol=1e-4,
                        atol=1e-5,
                    )

                print(
                    "CPU_PIPELINE "
                    + json.dumps(
                        {
                            "scenario": "worker-loss" if worker_loss else "normal",
                            "job_id": job["job_id"],
                            "api_restarted": True,
                            "submission_ids": [
                                attempt["submission_id"] for attempt in attempts
                            ],
                            "recovery_checkpoint": checkpoint["uri"]
                            if checkpoint
                            else None,
                            "first_epoch": result["first_epoch"],
                            "restored_adam_step": result["restored_optimizer_step"],
                            "model_id": model_id,
                            "version": output["version"],
                            "deployment": ready["deployment_identity"],
                            "artifact_uri": ready["artifact_uri"],
                            "predict_status": reply.status_code,
                            "predictions": output["predictions"],
                        },
                        sort_keys=True,
                    )
                )
