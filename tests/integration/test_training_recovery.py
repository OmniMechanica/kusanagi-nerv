"""Recover a job after a real worker SIGKILL using PostgreSQL, Ray and S3."""

import asyncio
import importlib.util
import io
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import httpx2
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from kusanagi_nerv.controller.service import Controller
from kusanagi_nerv.db.models import AttemptRow
from kusanagi_nerv.domain.execution import ExecutorUnavailable
from kusanagi_nerv.executors.ray_jobs import RayJobsSettings
from kusanagi_nerv.repositories.attempts import AttemptRepository
from tests.integration.test_api_restart import api_process
from tests.integration.test_ray_jobs import ray_info_file as ray_info_file

pytestmark = [pytest.mark.integration, pytest.mark.recovery]
SPEC = {
    "model": "tiny-mlp",
    "dataset": "synthetic-v1",
    "epochs": 5,
    "resources": {"cpus": 2, "gpus": 0, "memory_gib": 0.25},
}


@pytest.fixture
def object_store():
    if any(
        importlib.util.find_spec(name) is None for name in ("torch", "pyarrow", "boto3")
    ):
        pytest.skip("Install the recovery extra")
    if not os.environ.get("KUSANAGI_S3_ENDPOINT"):
        pytest.skip("Start local S3 storage and set KUSANAGI_S3_* variables")
    from botocore.exceptions import (
        ClientError,
        ConnectionClosedError,
        ConnectionError,
        ReadTimeoutError,
    )

    from kusanagi_nerv.training.object_store import (
        ObjectStore,
        ObjectStoreSettings,
        StorageUnavailable,
    )

    store = ObjectStore(ObjectStoreSettings.from_environment())
    probe_id = str(uuid4())
    owned = []
    try:
        # Container health and HeadBucket can succeed before the volume can
        # serve object writes. Check the whole data path with an owned key.
        deadline = time.monotonic() + 30
        while True:
            try:
                store.client.head_bucket(Bucket=store.settings.bucket)
                if not owned:
                    owned.append(probe_id)
                uri, digest = store.publish(f"jobs/{probe_id}/readiness", b"ready")
                assert store.read(uri, digest) == b"ready"
                break
            except (
                ConnectionError,
                ConnectionClosedError,
                ReadTimeoutError,
                ClientError,
                StorageUnavailable,
            ) as exc:
                cause = exc.__cause__ if isinstance(exc, StorageUnavailable) else exc
                if isinstance(cause, ClientError) and cause.response[
                    "ResponseMetadata"
                ]["HTTPStatusCode"] not in {404, 500, 503}:
                    raise
                if time.monotonic() >= deadline:
                    pytest.fail(
                        f"Local S3 bucket did not become ready ({type(cause).__name__})"
                    )
                time.sleep(0.25)
        yield store, owned
    finally:
        for job_id in owned:
            listing = store.client.list_objects_v2(
                Bucket=store.settings.bucket, Prefix=f"jobs/{job_id}/"
            )
            for obj in listing.get("Contents", []):
                store.client.delete_object(Bucket=store.settings.bucket, Key=obj["Key"])
        store.client.close()


async def attempt_rows(engine):
    async with engine.connect() as connection:
        return (
            (
                await connection.execute(
                    select(AttemptRow.__table__).order_by(AttemptRow.attempt_no)
                )
            )
            .mappings()
            .all()
        )


def read_checkpoint_in_fresh_process(checkpoint: dict) -> dict:
    script = """import io,json,sys,torch
from kusanagi_nerv.training.object_store import ObjectStore,ObjectStoreSettings
ref=json.loads(sys.argv[1])
state=torch.load(io.BytesIO(ObjectStore(ObjectStoreSettings.from_environment()).read(ref['uri'],ref['sha256'])),weights_only=True,map_location='cpu')
print(json.dumps({'epoch': state['epoch'], 'steps': sorted({int(value['step'].item()) for value in state['optimizer']['state'].values()}), 'history': len(state['history'])}))
"""
    process = subprocess.run(
        [sys.executable, "-I", "-c", script, json.dumps(checkpoint)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    return json.loads(process.stdout)


def test_s3_publication_is_atomic_and_checks_integrity(object_store) -> None:
    store, owned = object_store
    job_id = str(uuid4())
    owned.append(job_id)
    key = f"jobs/{job_id}/attempts/test/checkpoints/0001.pt"
    reference = store.publish(key, b"finished checkpoint")
    assert store.publish(key, b"finished checkpoint") == reference
    with pytest.raises(ValueError, match="integrity"):
        store.publish(key, b"different bytes")
    with pytest.raises(ValueError, match="integrity"):
        store.read(reference[0], "0" * 64)
    assert store.read(*reference) == b"finished checkpoint"


@pytest.mark.parametrize("budget", [1, 0])
async def test_real_worker_death_checkpoint_and_retry_budget(
    budget,
    postgres_url,
    engine,
    object_store,
    ray_info_file,
    tmp_path,
):
    import torch

    from kusanagi_nerv.executors.ray_training import RayTrainingExecutor
    from kusanagi_nerv.training.config import TrainingConfig
    from kusanagi_nerv.training.loop import load_model, synthetic_split, train

    store, owned = object_store
    config = json.loads(ray_info_file.read_text())
    attempts = AttemptRepository(async_sessionmaker(engine, expire_on_commit=False))
    executor = RayTrainingExecutor(
        attempts,
        RayJobsSettings(
            config["address"],
            config["cluster_id"],
            Path(config["token_file"]).read_text().strip(),
        ),
        store.settings,
        kill_after_epoch=2,
    )
    controller = Controller(attempts, executor)
    original_request = executor._request
    lost = False

    async def lose_first_submit_reply(method, path, **kwargs):
        nonlocal lost
        response = await original_request(method, path, **kwargs)
        if (
            method == "POST"
            and path == "/api/jobs/"
            and response.status_code == 200
            and not lost
        ):
            lost = True
            raise ExecutorUnavailable("injected lost submit acknowledgement")
        return response

    executor._request = lose_first_submit_reply
    try:
        with api_process(postgres_url) as (address, _):
            async with httpx2.AsyncClient(base_url=address, timeout=5) as api:
                submitted = await api.post(
                    "/v1/jobs", json={**SPEC, "max_retries": budget}
                )
                assert submitted.status_code == 202
                owned.append(submitted.json()["job_id"])
                location = submitted.headers["Location"]
                await controller.tick()
                assert lost
                [pending] = await attempt_rows(engine)
                assert (
                    pending["last_observation"] == "unknown"
                    and pending["finished_at"] is None
                )
                assert (await api.get(location)).json()["attempt"] == 1

                async with asyncio.timeout(120):
                    while True:
                        await controller.tick()
                        view = (await api.get(location)).json()
                        if view["status"] in {"succeeded", "failed"}:
                            break
                        await asyncio.sleep(0.1)

                rows = await attempt_rows(engine)
                first = rows[0]
                assert first["id"] == pending["id"]
                assert first["last_observation"] == "failed"
                path = f"/api/jobs/{first['submission_id']}"
                ray_data = (await executor.client.get(path)).json()
                first_logs = (await executor.client.get(path + "/logs")).json()["logs"]
                assert ray_data["driver_exit_code"] == 75, first_logs[-10000:]
                assert first["failure_kind"] == "transient", first
                checkpoint = first["published_checkpoint"]
                assert checkpoint["epoch"] == 2
                assert first["resume_checkpoint"] is None
                assert read_checkpoint_in_fresh_process(checkpoint) == {
                    "epoch": 2,
                    "steps": [12],
                    "history": 2,
                }
                assert "KUSANAGI_RECOVERY_RESULT " not in first_logs

                if budget == 0:
                    assert (
                        view["status"] == "failed" and view["attempt"] == len(rows) == 1
                    )
                    assert (
                        view["checkpoint_uri"] == checkpoint["uri"]
                        and view["artifact_uri"] is None
                    )
                    await controller.tick()
                    assert len(await attempt_rows(engine)) == 1
                    return

                assert (
                    view["status"] == "succeeded" and view["attempt"] == len(rows) == 2
                )
                second = rows[1]
                assert (
                    second["id"] != first["id"]
                    and second["submission_id"] != first["submission_id"]
                )
                assert second["resume_checkpoint"] == checkpoint
                assert second["published_checkpoint"]["epoch"] == SPEC["epochs"]
                assert view["checkpoint_uri"] == second["published_checkpoint"]["uri"]
                result = second["execution_result"]
                assert result["completed_steps"] == 5 and result["first_epoch"] == 3
                assert result["restored_optimizer_step"] == 12
                assert view["artifact_uri"] == result["artifact_uri"]
                assert second["submission_id"] in view["artifact_uri"]
                model_bytes = store.read(view["artifact_uri"], result["checksum"])
                payload = torch.load(
                    io.BytesIO(model_bytes), weights_only=True, map_location="cpu"
                )
                source = torch.load(
                    io.BytesIO(store.read(checkpoint["uri"], checkpoint["sha256"])),
                    weights_only=True,
                    map_location="cpu",
                )
                assert payload["metrics"]["epochs"][:2] == source["history"]
                assert [row["epoch"] for row in payload["metrics"]["epochs"]] == list(
                    range(1, 6)
                )
                artifact_path = tmp_path / "resumed.pt"
                artifact_path.write_bytes(model_bytes)
                serial = tmp_path / "uninterrupted.pt"
                train(TrainingConfig(epochs=5, batch_size=64), serial)
                _, _, evaluation, _ = synthetic_split()
                with torch.inference_mode():
                    torch.testing.assert_close(
                        load_model(artifact_path)(evaluation),
                        load_model(serial)(evaluation),
                        rtol=1e-4,
                        atol=1e-5,
                    )
    finally:
        await executor.aclose()
