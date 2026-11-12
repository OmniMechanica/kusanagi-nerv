"""Prove committed jobs survive an actual API process crash, not just a new object."""

import asyncio
import os
import re
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import httpx2
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine

from kusanagi_nerv.db.models import AttemptRow, SimulatedExecutionRow

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


@contextmanager
def api_process(database_url: str) -> Iterator[tuple[str, subprocess.Popen[str]]]:
    with tempfile.TemporaryFile(mode="w+") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "kusanagi_nerv.api.app:create_app",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                "0",
            ],
            cwd=ROOT,
            env={**os.environ, "KUSANAGI_DATABASE_URL": database_url},
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                log.seek(0)
                output = log.read()
                address = re.search(
                    r"Uvicorn running on (http://127\.0\.0\.1:\d+)", output
                )
                if address:
                    yield address.group(1), process
                    break
                if process.poll() is not None:
                    pytest.fail(f"API exited during startup: {output}")
                # Poll only for process readiness, never to coordinate a DB race.
                time.sleep(0.05)
            else:
                pytest.fail("API did not start within 15 seconds")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def test_accepted_job_survives_api_process_crash(postgres_url: str) -> None:
    with (
        api_process(postgres_url) as (address, process),
        httpx2.Client(base_url=address, timeout=5) as client,
    ):
        created = client.post(
            "/v1/jobs",
            headers={"Idempotency-Key": "survive-process-crash"},
            json={
                "model": "tiny-mlp",
                "dataset": "synthetic-v1",
                "epochs": 5,
                "resources": {"cpus": 2, "gpus": 0, "memory_gib": 2},
                "max_retries": 2,
            },
        )
        assert created.status_code == 202
        location = created.headers["Location"]
        original = client.get(location)
        assert original.status_code == 200
        process.kill()
        process.wait(timeout=5)

    with (
        api_process(postgres_url) as (address, _),
        httpx2.Client(base_url=address, timeout=5) as client,
    ):
        recovered = client.get(location)
        assert recovered.status_code == 200
        assert recovered.json() == original.json()
        assert recovered.json()["status"] == "queued"
        assert recovered.json()["attempt"] == 0
        replay = client.post(
            "/v1/jobs",
            headers={"Idempotency-Key": "survive-process-crash"},
            json={
                "model": "tiny-mlp",
                "dataset": "synthetic-v1",
                "epochs": 5,
                "resources": {"cpus": 2.0, "gpus": 0, "memory_gib": 2.0},
                "max_retries": 2,
            },
        )
        assert replay.status_code == 202 and replay.headers["Location"] == location
        assert client.get(location).json() == original.json()


@contextmanager
def controller_process(database_url: str) -> Iterator[subprocess.Popen[str]]:
    with tempfile.TemporaryFile(mode="w+") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "kusanagi_nerv.controller",
                "--executor",
                "simulated",
                "--lease-seconds",
                "3",
                "--poll-seconds",
                "0.1",
                "--executor-timeout-seconds",
                "0.5",
                "--simulation-seconds",
                "2",
            ],
            cwd=ROOT,
            env={**os.environ, "KUSANAGI_DATABASE_URL": database_url},
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            yield process
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


async def test_controller_process_crash_reconciles_same_submission(
    postgres_url: str,
    engine: AsyncEngine,
) -> None:
    with api_process(postgres_url) as (address, _):
        async with httpx2.AsyncClient(base_url=address, timeout=5) as client:
            response = await client.post(
                "/v1/jobs",
                json={
                    "model": "tiny-mlp",
                    "dataset": "synthetic-v1",
                    "epochs": 1,
                    "resources": {"cpus": 1, "gpus": 0, "memory_gib": 1},
                },
            )
            # Admission works while no controller is running.
            assert response.status_code == 202
            location = response.headers["Location"]
            assert (await client.get(location)).json()["attempt"] == 0

            async def wait_status(status: str, process: subprocess.Popen[str]) -> dict:
                async with asyncio.timeout(15):
                    while True:
                        assert process.poll() is None, "Controller exited unexpectedly"
                        result = (await client.get(location)).json()
                        if result["status"] == status:
                            return result
                        await asyncio.sleep(0.05)

            with controller_process(postgres_url) as first:
                await wait_status("running", first)
                async with engine.connect() as connection:
                    original = (
                        (await connection.execute(select(AttemptRow.__table__)))
                        .mappings()
                        .one()
                    )
                first.kill()  # Actual process death, no graceful lease release.
                first.wait(timeout=5)
            with controller_process(postgres_url) as second:
                result = await wait_status("succeeded", second)
                assert result["attempt"] == 1 and result["result_model_id"] is None
                async with engine.connect() as connection:
                    recovered = (
                        (await connection.execute(select(AttemptRow.__table__)))
                        .mappings()
                        .one()
                    )
                    assert recovered["id"] == original["id"]
                    assert recovered["submission_id"] == original["submission_id"]
                    assert (
                        recovered["submission_intent_at"]
                        == original["submission_intent_at"]
                    )
                    assert recovered["lease_owner"] != original["lease_owner"]
                    assert recovered["lease_epoch"] == original["lease_epoch"] + 1
                    assert recovered["finished_at"] is not None
                    assert (
                        await connection.scalar(
                            select(func.count()).select_from(SimulatedExecutionRow)
                        )
                        == 1
                    )
