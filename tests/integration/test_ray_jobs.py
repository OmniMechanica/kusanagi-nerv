"""Actual API/controller processes, independent Ray Jobs runtime and PostgreSQL."""

import asyncio
import importlib.util
import json
import os
import shlex
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

import httpx2
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from kusanagi_nerv.controller.service import Controller
from kusanagi_nerv.db.models import AttemptRow
from kusanagi_nerv.domain.execution import ExecutionStatus
from kusanagi_nerv.executors.ray_jobs import (
    RayJobsExecutor,
    RayJobsSettings,
    workload_events,
)
from kusanagi_nerv.repositories.attempts import AttemptRepository
from tests.integration.test_api_restart import ROOT, api_process

pytestmark = [pytest.mark.integration, pytest.mark.ray_jobs]
SPEC = {
    "model": "tiny-mlp",
    "dataset": "synthetic-v1",
    "epochs": 8,
    "resources": {"cpus": 1, "gpus": 0, "memory_gib": 0.25},
    "max_retries": 2,
}


@pytest.fixture(scope="module")
def ray_info_file(tmp_path_factory):
    if (
        importlib.util.find_spec("ray") is None
        or importlib.util.find_spec("aiohttp") is None
    ):
        pytest.skip("Install the ray-jobs extra for real Ray Jobs evidence")
    configured = os.environ.get("KUSANAGI_TEST_RAY_INFO")
    if configured:
        yield Path(configured)
        return
    path = tmp_path_factory.mktemp("ray-jobs") / "cluster.json"
    with tempfile.TemporaryFile(mode="w+") as log:
        env = {
            key: value
            for key, value in os.environ.items()
            if key not in {"KUSANAGI_DATABASE_URL", "KUSANAGI_TEST_DATABASE_URL"}
        }
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "kusanagi_nerv.runtime.local",
                "--info-file",
                str(path),
                "--port",
                "0",
            ],
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            deadline = time.monotonic() + 90
            while not path.exists():
                if process.poll() is not None or time.monotonic() > deadline:
                    log.seek(0)
                    pytest.fail("Ray Jobs cluster did not start: " + log.read())
                time.sleep(0.1)
            yield path
        finally:
            stop(process)
    assert not path.exists()


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


@contextmanager
def controller_process(database_url, ray_info_file, *, crash_after_submit=False):
    command = [sys.executable, "-m", "kusanagi_nerv.controller"]
    if crash_after_submit:
        # Test-only deterministic crash, after remote acceptance and before the
        # controller commits observe(). No production failpoint or timing sleep.
        script = """import os
from kusanagi_nerv.executors.ray_jobs import RayJobsExecutor
from kusanagi_nerv.controller.__main__ import main
original = RayJobsExecutor.submit
async def crash(self, claim):
    await original(self, claim)
    os._exit(23)
RayJobsExecutor.submit = crash
main()
"""
        command = [sys.executable, "-c", script]
    command += [
        "--executor",
        "ray",
        "--ray-info-file",
        str(ray_info_file),
        "--lease-seconds",
        "6",
        "--poll-seconds",
        "0.1",
        "--executor-timeout-seconds",
        "1.5",
    ]
    with tempfile.TemporaryFile(mode="w+") as log:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env={**os.environ, "KUSANAGI_DATABASE_URL": database_url},
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            yield process
        finally:
            stop(process)


@pytest.fixture
async def ray_executor(engine, ray_info_file):
    info = json.loads(ray_info_file.read_text())
    store = AttemptRepository(async_sessionmaker(engine, expire_on_commit=False))
    executor = RayJobsExecutor(
        store,
        RayJobsSettings(
            info["address"],
            info["cluster_id"],
            Path(info["token_file"]).read_text().strip(),
        ),
    )
    try:
        yield executor
    finally:
        await executor.aclose()


async def attempt_row(engine):
    async with engine.connect() as connection:
        return (await connection.execute(select(AttemptRow.__table__))).mappings().one()


async def wait_status(client, location, status, *, process=None):
    async with asyncio.timeout(60):
        while True:
            if process is not None:
                assert process.poll() is None, "Controller exited unexpectedly"
            view = (await client.get(location)).json()
            if view["status"] == status:
                return view
            await asyncio.sleep(0.05)


async def assert_one_workload(executor, row):
    response = await executor.client.get(f"/api/jobs/{row['submission_id']}/logs")
    assert response.status_code == 200
    events = workload_events(response.json()["logs"], row["submission_id"])
    assert sum(event["event"] == "started" for event in events) == 1
    assert sum(event["event"] == "completed" for event in events) == 1
    return events


async def test_api_controller_ray_result_survives_controller_process_crash(
    postgres_url, engine, ray_executor, ray_info_file
):
    with api_process(postgres_url) as (address, api):
        async with httpx2.AsyncClient(base_url=address, timeout=5) as client:
            response = await client.post("/v1/jobs", json=SPEC)
            assert response.status_code == 202
            location = response.headers["Location"]
            assert (await client.get(location)).json()["attempt"] == 0
            with controller_process(postgres_url, ray_info_file) as first:
                await wait_status(client, location, "running", process=first)
                original = await attempt_row(engine)
                first.kill()
                first.wait(timeout=5)
            assert (await client.get(location)).status_code == 200
            with controller_process(postgres_url, ray_info_file) as second:
                final = await wait_status(client, location, "succeeded", process=second)
                recovered = await attempt_row(engine)
                assert final["attempt"] == 1
                assert recovered["id"] == original["id"]
                assert recovered["submission_id"] == original["submission_id"]
                assert recovered["lease_epoch"] == original["lease_epoch"] + 1
                assert recovered["executor_target"] == ray_executor.target
                assert recovered["execution_result"]["worker_pid"] not in {
                    api.pid,
                    first.pid,
                    second.pid,
                    os.getpid(),
                }
                assert recovered["execution_result"]["completed_steps"] == 8
                await assert_one_workload(ray_executor, recovered)


async def test_crash_after_ray_acceptance_before_ledger_acknowledgement(
    postgres_url, engine, ray_executor, ray_info_file
):
    with api_process(postgres_url) as (address, _):
        async with httpx2.AsyncClient(base_url=address, timeout=5) as client:
            response = await client.post("/v1/jobs", json=SPEC)
            location = response.headers["Location"]
            with controller_process(
                postgres_url, ray_info_file, crash_after_submit=True
            ) as first:
                async with asyncio.timeout(45):
                    while first.poll() is None:
                        await asyncio.sleep(0.05)
                assert first.returncode == 23
            original = await attempt_row(engine)
            assert original["accepted_at"] is None
            assert original["dispatch_started_at"] is not None
            assert (
                await ray_executor.client.get(f"/api/jobs/{original['submission_id']}")
            ).status_code == 200
            with controller_process(postgres_url, ray_info_file) as second:
                final = await wait_status(client, location, "succeeded", process=second)
            recovered = await attempt_row(engine)
            assert final["attempt"] == 1
            assert recovered["submission_id"] == original["submission_id"]
            assert recovered["lease_epoch"] == original["lease_epoch"] + 1
            await assert_one_workload(ray_executor, recovered)


async def test_concurrent_duplicate_submissions_and_completion_between_polls(
    postgres_url, engine, ray_executor
):
    with api_process(postgres_url) as (address, _):
        async with httpx2.AsyncClient(base_url=address, timeout=5) as client:
            response = await client.post("/v1/jobs", json=SPEC)
            claim = await ray_executor.store.claim_next(
                owner="manual",
                executor_kind=ray_executor.kind,
                executor_target=ray_executor.target,
                lease_seconds=60,
            )
            await asyncio.gather(ray_executor.submit(claim), ray_executor.submit(claim))
            async with asyncio.timeout(45):
                while (
                    await ray_executor.lookup(claim.submission_id)
                ).status != ExecutionStatus.SUCCEEDED:
                    await asyncio.sleep(0.05)
            controller = Controller(ray_executor.store, ray_executor)
            controller.claim = claim
            await controller.tick()
            final = (await client.get(response.headers["Location"])).json()
            assert final["status"] == "succeeded" and final["attempt"] == 1
            row = await attempt_row(engine)
            assert row["started_at"] is None  # No fabricated RUNNING observation.
            assert row["execution_result"]["completed_steps"] == 8
            await assert_one_workload(ray_executor, row)


async def test_cancel_before_running_observation_waits_for_delayed_submit(
    postgres_url, engine, ray_executor
):
    with api_process(postgres_url) as (address, _):
        async with httpx2.AsyncClient(base_url=address, timeout=5) as client:
            response = await client.post("/v1/jobs", json=SPEC)
            location = response.headers["Location"]
            claim = await ray_executor.store.claim_next(
                owner="manual",
                executor_kind=ray_executor.kind,
                executor_target=ray_executor.target,
                lease_seconds=60,
            )
            arrived, release = asyncio.Event(), asyncio.Event()
            original = ray_executor._request
            submitted = {}

            async def delayed(method, path, **kwargs):
                if method == "POST" and path == "/api/jobs/":
                    submitted.update(kwargs["json"])
                    arrived.set()
                    await release.wait()
                return await original(method, path, **kwargs)

            ray_executor._request = delayed
            pending = asyncio.create_task(ray_executor.submit(claim))
            try:
                await asyncio.wait_for(arrived.wait(), timeout=10)
                cancelled = await client.post(location + "/cancel")
                assert cancelled.json()["status"] == "cancelling"
                assert (
                    await ray_executor.cancel(claim.submission_id)
                ).status == ExecutionStatus.UNKNOWN
                assert (await client.get(location)).json()["status"] == "cancelling"
            finally:
                release.set()
                await pending
                ray_executor._request = original
            controller = Controller(ray_executor.store, ray_executor)
            controller.claim = claim
            async with asyncio.timeout(45):
                while controller.claim is not None:
                    await controller.tick()
                    await asyncio.sleep(0.05)
            row = await attempt_row(engine)
            assert row["dispatch_revoked_at"] is not None
            assert row["started_at"] is None
            assert (await client.get(location)).json()["status"] == "cancelled"
            # A truly late duplicate wire request is rejected by the retained Ray
            # identity, even after the controller has committed cancellation.
            repeated = await ray_executor.client.post("/api/jobs/", json=submitted)
            assert repeated.status_code != 200
            assert (
                await ray_executor.lookup(claim.submission_id)
            ).status == ExecutionStatus.CANCELLED


async def test_confirmed_ray_failure_does_not_retry_without_transient_evidence(
    postgres_url, engine, ray_executor
):
    # Inject a real nonzero driver exit, keeping the normal identity/metadata
    # contract. This override exists only in this test.
    ray_executor.entrypoint = shlex.join(
        [sys.executable, "-c", "raise RuntimeError('injected workload failure')"]
    )
    with api_process(postgres_url) as (address, _):
        async with httpx2.AsyncClient(base_url=address, timeout=5) as client:
            response = await client.post("/v1/jobs", json=SPEC)
            controller = Controller(ray_executor.store, ray_executor)
            async with asyncio.timeout(45):
                while True:
                    await controller.tick()
                    view = (await client.get(response.headers["Location"])).json()
                    if view["status"] == "failed":
                        break
                    await asyncio.sleep(0.05)
            assert view["attempt"] == 1 and view["max_retries"] == 2
            row = await attempt_row(engine)
            assert row["failure_kind"] == "permanent"
            assert row["execution_result"] is None
            await controller.tick()
            assert controller.claim is None
