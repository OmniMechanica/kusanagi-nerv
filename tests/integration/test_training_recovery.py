"""Recover a job after a real worker SIGKILL using PostgreSQL, Ray and S3."""

import asyncio
import hashlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx2
import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from kusanagi_nerv.api.app import create_app
from kusanagi_nerv.controller.service import Controller
from kusanagi_nerv.db.models import AttemptRow, JobRow
from kusanagi_nerv.domain.execution import (
    CheckpointRef,
    ExecutionStatus,
    ExecutorUnavailable,
    FailureKind,
)
from kusanagi_nerv.executors.ray_jobs import RayJobsSettings
from kusanagi_nerv.repositories.attempts import AttemptRepository
from kusanagi_nerv.training.recovery_config import INPUT_ENV, WORKER_LOST_EXIT
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


def dispatch_record():
    # No database or Ray server is needed to exercise the adapter's wire checks.
    return SimpleNamespace(
        claim=SimpleNamespace(
            job=SimpleNamespace(job_id=uuid4(), spec=SimpleNamespace(epochs=5)),
            submission_id=f"kusanagi-{uuid4()}",
            attempt_id=uuid4(),
            resume_checkpoint=None,
        ),
        payload_hash="a" * 64,
        started=True,
        revoked=True,
    )


async def test_interrupted_s3_download_preserves_uncertain_outcome(monkeypatch):
    pytest.importorskip("boto3")
    from botocore.response import StreamingBody
    from botocore.stub import Stubber
    from urllib3.exceptions import ProtocolError

    from kusanagi_nerv.executors.ray_training import RayTrainingExecutor
    from kusanagi_nerv.training.object_store import ObjectStoreSettings

    class InterruptedStream(io.BytesIO):
        def read(self, size=-1):
            raise ProtocolError("Connection broke during the object body")

    executor = RayTrainingExecutor(
        None,
        RayJobsSettings("http://127.0.0.1:8265", "session_stream", "a" * 64),
        ObjectStoreSettings("http://127.0.0.1:58333", "test-bucket", "fake", "fake"),
    )
    record = dispatch_record()
    checkpoint = CheckpointRef(executor.objects.uri("previous.pt"), "b" * 64, 2)
    record.claim.resume_checkpoint = checkpoint
    raw = InterruptedStream()
    monkeypatch.setattr(
        executor.objects.client,
        "list_objects_v2",
        lambda **_: {"IsTruncated": False},
    )
    try:
        with Stubber(executor.objects.client) as s3:
            s3.add_response("get_object", {"Body": StreamingBody(raw, 128)})
            for code, status in [
                ("NoSuchBucket", 404),
                ("404", 404),
                ("AccessDenied", 403),
            ]:
                s3.add_client_error(
                    "get_object", service_error_code=code, http_status_code=status
                )
            controller = Controller(None, executor)
            for _ in range(4):
                observation = await controller._call(
                    lambda: executor._failed(
                        record, {"driver_exit_code": WORKER_LOST_EXIT}
                    )
                )
                assert observation.status == ExecutionStatus.UNKNOWN
                assert not observation.checkpoint_reconciled
            s3.assert_no_pending_responses()
        assert raw.closed
    finally:
        await executor.aclose()


async def test_cancel_sends_ray_stop_without_consulting_s3(monkeypatch):
    pytest.importorskip("boto3")
    from kusanagi_nerv.executors.ray_training import RayTrainingExecutor
    from kusanagi_nerv.training.distributed_config import START_PREFIX
    from kusanagi_nerv.training.object_store import (
        ObjectStoreSettings,
        StorageUnavailable,
    )
    from kusanagi_nerv.training.recovery_config import CHECKPOINT_PREFIX

    record = dispatch_record()
    settings = RayJobsSettings("http://127.0.0.1:8265", "session_cancel", "a" * 64)
    stops = []
    status = "RUNNING"

    async def revoke(*_):
        return record

    def ray_response(request):
        path = request.url.path
        if path == "/api/version":
            return httpx2.Response(200, json={"session_name": settings.cluster_id})
        if path.endswith("/stop"):
            stops.append(request.method)
            return httpx2.Response(200, json={"stopped": True})
        if path.endswith("/logs"):
            start = {
                "submission_id": record.claim.submission_id,
                "rank": 0,
                "world_size": 2,
                "device": "cpu",
                "first_epoch": 1,
            }
            checkpoint = {
                "submission_id": record.claim.submission_id,
                "epoch": 2,
                "uri": executor.objects.uri(
                    f"{executor._prefix(record)}/checkpoints/0002.pt"
                ),
                "sha256": "b" * 64,
            }
            return httpx2.Response(
                200,
                json={
                    "logs": START_PREFIX
                    + json.dumps(start)
                    + "\n"
                    + CHECKPOINT_PREFIX
                    + json.dumps(checkpoint)
                },
            )
        return httpx2.Response(
            200,
            json={
                "submission_id": record.claim.submission_id,
                "entrypoint": executor.entrypoint,
                "metadata": executor._metadata(record),
                "status": status,
            },
        )

    executor = RayTrainingExecutor(
        SimpleNamespace(revoke_dispatch=revoke),
        settings,
        ObjectStoreSettings("http://127.0.0.1:58333", "test-bucket", "fake", "fake"),
        transport=httpx2.MockTransport(ray_response),
    )

    def outage(_):
        raise StorageUnavailable("Injected controller-side S3 outage")

    monkeypatch.setattr(executor.objects, "digest", outage)
    try:
        pending = await executor.cancel(record.claim.submission_id)
        assert stops == ["POST"] and pending.status == ExecutionStatus.PENDING
        status = "STOPPED"
        confirmed = await executor.cancel(record.claim.submission_id)
        assert confirmed.status == ExecutionStatus.CANCELLED and stops == ["POST"]
    finally:
        await executor.aclose()


async def test_success_requires_matching_artifact_bytes(object_store):
    from kusanagi_nerv.executors.ray_training import RayTrainingExecutor
    from kusanagi_nerv.training.recovery_config import RECOVERY_RESULT_PREFIX

    store, owned = object_store
    record = dispatch_record()
    owned.append(str(record.claim.job.job_id))
    executor = RayTrainingExecutor(
        None,
        RayJobsSettings("http://127.0.0.1:8265", "session_integrity", "a" * 64),
        store.settings,
    )
    try:
        prefix = executor._prefix(record)
        checkpoint_key = f"{prefix}/checkpoints/0005.pt"
        checkpoint = b"complete checkpoint"
        store.publish(checkpoint_key, checkpoint)
        key = f"{prefix}/model.pt"
        expected = b"intact model"
        checksum = hashlib.sha256(expected).hexdigest()
        metadata = {"sha256": checksum}
        # Deliberately create an inaccurate HEAD response on an owned test key.
        store.client.put_object(
            Bucket=store.settings.bucket,
            Key=key,
            Body=b"wrong bytes!",
            Metadata=metadata,
        )
        assert store.digest(key) == checksum
        logs = RECOVERY_RESULT_PREFIX + json.dumps(
            {
                "submission_id": record.claim.submission_id,
                "worker_pid": 123,
                "completed_steps": 5,
                "artifact_uri": store.uri(key),
                "first_epoch": 1,
                "restored_optimizer_step": 0,
                "checksum": checksum,
            }
        )
        rejected = await executor._from_logs(record, "SUCCEEDED", logs)
        assert rejected.status == ExecutionStatus.UNKNOWN and rejected.result is None
        store.client.put_object(
            Bucket=store.settings.bucket, Key=key, Body=expected, Metadata=metadata
        )
        checkpoint_metadata = {"sha256": hashlib.sha256(checkpoint).hexdigest()}
        store.client.put_object(
            Bucket=store.settings.bucket,
            Key=checkpoint_key,
            Body=b"corrupt checkpoint!",
            Metadata=checkpoint_metadata,
        )
        rejected = await executor._from_logs(record, "SUCCEEDED", logs)
        assert rejected.status == ExecutionStatus.UNKNOWN and rejected.result is None
        store.client.put_object(
            Bucket=store.settings.bucket,
            Key=checkpoint_key,
            Body=checkpoint,
            Metadata=checkpoint_metadata,
        )
        accepted = await executor._from_logs(record, "SUCCEEDED", logs)
        assert (
            accepted.status == ExecutionStatus.SUCCEEDED
            and accepted.result.checksum == checksum
        )
    finally:
        await executor.aclose()


async def test_worker_loss_skips_checkpoints_with_invalid_metadata(object_store):
    from kusanagi_nerv.executors.ray_training import RayTrainingExecutor

    store, owned = object_store
    record = dispatch_record()
    job_id = str(record.claim.job.job_id)
    owned.append(job_id)
    inherited = store.publish(
        f"jobs/{job_id}/attempts/previous/checkpoints/0002.pt", b"inherited checkpoint"
    )
    record.claim.resume_checkpoint = CheckpointRef(*inherited, 2)
    executor = RayTrainingExecutor(
        None,
        RayJobsSettings("http://127.0.0.1:8265", "session_retry", "a" * 64),
        store.settings,
    )
    try:
        prefix = executor._prefix(record)
        store.client.put_object(
            Bucket=store.settings.bucket,
            Key=f"{prefix}/checkpoints/0004.pt",
            Body=b"checkpoint with no digest metadata",
        )
        failure = {"driver_exit_code": WORKER_LOST_EXIT}
        fallback = await executor._failed(record, failure)
        assert fallback.status == ExecutionStatus.FAILED
        assert fallback.failure_kind == FailureKind.TRANSIENT
        assert fallback.checkpoint is None
        assert fallback.retry_checkpoint == record.claim.resume_checkpoint

        earlier = store.publish(f"{prefix}/checkpoints/0003.pt", b"verified checkpoint")
        recovered = await executor._failed(record, failure)
        assert (
            recovered.checkpoint
            == recovered.retry_checkpoint
            == CheckpointRef(*earlier, 3)
        )
    finally:
        await executor.aclose()


@pytest.mark.parametrize("lost_inherited", [False, True], ids=["fallback", "deleted"])
async def test_worker_loss_reconciles_published_checkpoint_in_api(
    lost_inherited, object_store, engine, repository, monkeypatch
):
    from kusanagi_nerv.executors.ray_training import RayTrainingExecutor
    from kusanagi_nerv.training.object_store import attempt_prefix, checkpoint_key

    store, owned = object_store
    attempts = AttemptRepository(async_sessionmaker(engine, expire_on_commit=False))
    executor = RayTrainingExecutor(
        attempts,
        RayJobsSettings("http://127.0.0.1:8265", "session_reconcile", "a" * 64),
        store.settings,
    )
    controller = Controller(attempts, executor)

    async def observe_worker_loss(submission_id):
        record = await attempts.read_dispatch(
            submission_id, executor.target, executor.kind
        )
        assert record is not None and record.started
        return await executor._failed(record, {"driver_exit_code": WORKER_LOST_EXIT})

    monkeypatch.setattr(executor, "lookup", observe_worker_loss)
    claim_options = {
        "owner": controller.owner,
        "executor_kind": executor.kind,
        "executor_target": executor.target,
        "lease_seconds": 30,
    }

    try:
        app = create_app(repository)
        async with (
            app.router.lifespan_context(app),
            httpx2.AsyncClient(
                transport=httpx2.ASGITransport(app), base_url="http://test"
            ) as api,
        ):
            submitted = await api.post(
                "/v1/jobs", json={**SPEC, "max_retries": int(lost_inherited)}
            )
            assert submitted.status_code == 202
            job_id = submitted.json()["job_id"]
            owned.append(job_id)
            location = submitted.headers["Location"]
            first = await attempts.claim_next(**claim_options)
            assert first is not None
            assert await attempts.begin_dispatch(first, executor.kind)
            prefix = attempt_prefix(job_id, first.submission_id)
            earlier_key = checkpoint_key(prefix, 3)
            earlier = CheckpointRef(*store.publish(earlier_key, b"good checkpoint"), 3)

            if lost_inherited:
                controller.claim = first
                await controller.tick()
                retrying = (await api.get(location)).json()
                assert retrying["status"] == "retrying"
                assert retrying["checkpoint_uri"] == earlier.uri
                async with engine.begin() as connection:
                    await connection.execute(
                        update(JobRow)
                        .where(JobRow.job_id == first.job.job_id)
                        .values(retry_not_before=func.clock_timestamp())
                    )
                second = await attempts.claim_next(**claim_options)
                assert second is not None and second.resume_checkpoint == earlier
                assert await attempts.begin_dispatch(second, executor.kind)
                store.client.delete_object(
                    Bucket=store.settings.bucket, Key=earlier_key
                )
                controller.claim = second
            else:
                latest_key = checkpoint_key(prefix, 4)
                latest = CheckpointRef(
                    *store.publish(latest_key, b"original epoch 4"), 4
                )
                controller.claim = await attempts.observe(
                    first, ExecutionStatus.RUNNING, checkpoint=latest
                )
                assert (await api.get(location)).json()["checkpoint_uri"] == latest.uri
                store.client.put_object(
                    Bucket=store.settings.bucket,
                    Key=latest_key,
                    Body=b"damaged epoch 4!",
                    Metadata={"sha256": latest.sha256},
                )

            await controller.tick()
            assert controller.claim is None
            view = (await api.get(location)).json()
            assert view["status"] == "failed"
            assert view["checkpoint_uri"] == (None if lost_inherited else earlier.uri)
            assert view["attempt"] == int(lost_inherited) + 1
            rows = await attempt_rows(engine)
            if lost_inherited:
                assert rows[0]["published_checkpoint"] == asdict(earlier)
                assert rows[1]["resume_checkpoint"] == asdict(earlier)
                assert rows[1]["retry_checkpoint"] is None
                assert rows[1]["failure_kind"] == FailureKind.PERMANENT
            else:
                assert rows[0]["published_checkpoint"] == asdict(latest)
                assert rows[0]["retry_checkpoint"] == asdict(earlier)
    finally:
        await executor.aclose()


@pytest.mark.parametrize("budget", [2, 1, 0])
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
    original_payload = executor._payload
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

    def interrupt_attempt_two(record):
        payload = original_payload(record)
        if budget == 2 and record.claim.job.attempt == 2:
            env = payload["runtime_env"]["env_vars"]
            inputs = json.loads(env[INPUT_ENV])
            inputs["recovery"]["kill_after_epoch"] = 2
            env[INPUT_ENV] = json.dumps(inputs)
        return payload

    executor._payload = interrupt_attempt_two
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
                assert first["retry_checkpoint"] == checkpoint
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
                    view["status"] == "succeeded"
                    and view["attempt"] == len(rows) == budget + 1
                )
                if budget == 2:
                    second = rows[1]
                    assert second["last_observation"] == "failed"
                    assert second["failure_kind"] == "transient"
                    assert (
                        second["resume_checkpoint"]
                        == second["retry_checkpoint"]
                        == checkpoint
                    )
                    assert second["published_checkpoint"] is None
                    failed_again = await executor.client.get(
                        f"/api/jobs/{second['submission_id']}"
                    )
                    assert failed_again.json()["driver_exit_code"] == 75
                    second_prefix = (
                        f"jobs/{first['job_id']}/attempts/{second['submission_id']}"
                    )
                    assert (
                        store.latest_checkpoint(second_prefix, SPEC["epochs"]) is None
                    )
                resumed = rows[-1]
                assert (
                    len({row["id"] for row in rows}) == budget + 1
                    and len({row["submission_id"] for row in rows}) == budget + 1
                )
                assert resumed["resume_checkpoint"] == checkpoint
                assert resumed["published_checkpoint"]["epoch"] == SPEC["epochs"]
                assert view["checkpoint_uri"] == resumed["published_checkpoint"]["uri"]
                result = resumed["execution_result"]
                assert result["completed_steps"] == 5 and result["first_epoch"] == 3
                assert result["restored_optimizer_step"] == 12
                assert view["artifact_uri"] == result["artifact_uri"]
                assert resumed["submission_id"] in view["artifact_uri"]
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
