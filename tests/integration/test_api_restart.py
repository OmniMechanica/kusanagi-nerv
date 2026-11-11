"""Prove committed jobs survive an actual API process crash, not just a new object."""

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
