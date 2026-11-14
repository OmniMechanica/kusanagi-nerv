"""Real CPU processes, object store, actor state, backpressure and failures."""

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.ray,
    pytest.mark.skipif(
        importlib.util.find_spec("ray") is None, reason="Install the ray extra"
    ),
]


def read_record(result: subprocess.CompletedProcess[str], prefix: str) -> dict:
    lines = [
        line.removeprefix(prefix)
        for line in result.stdout.splitlines()
        if line.startswith(prefix)
    ]
    assert len(lines) == 1, result.stdout + result.stderr
    return json.loads(lines[0])


def run_example(*arguments: str) -> subprocess.CompletedProcess[str]:
    # Each CLI invocation owns and shuts down its fresh local cluster, including
    # on task failure or a batch deadline. This timeout is an outer test bound.
    result = subprocess.run(
        [sys.executable, "-m", "kusanagi_nerv.examples.ray_core", *arguments],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    cleanup = read_record(result, "RAY_DEMO_CLEANUP ")
    assert cleanup["ray_initialized"] is False, result.stdout + result.stderr
    assert not Path(cleanup["runtime_directory"]).exists()
    return result


@pytest.mark.parametrize(("workers", "window"), [(2, 4), (1, 3), (2, 1)])
def test_tasks_and_actor_with_cpu_and_submission_limits(
    workers: int, window: int
) -> None:
    result = run_example(
        "--tasks", "6", "--workers", str(workers), "--max-outstanding", str(window)
    )
    assert result.returncode == 0, result.stdout + result.stderr
    report = read_record(result, "RAY_DEMO_REPORT ")
    assert report["runtime_shutdown"] is True
    assert report["resources"]["CPU"] == workers + 1
    assert report["resources"].get("GPU", 0) == 0
    assert 1 <= report["max_running"] <= min(workers, window)
    assert report["peak_outstanding"] == window
    assert report["backpressure_waits"] == 6 - window
    before, after = report["actor_before"], report["actor_after"]
    assert before["pid"] == after["pid"] != report["driver_pid"]
    assert before["completed"] == before["value_sum"] == 0
    assert after["completed"] == 6
    assert sorted(after["completion_order"]) == list(range(6))
    expected_base = sum(value * value for value in range(64))
    assert after["value_sum"] == sum(expected_base + i for i in range(6))
    assert [item["index"] for item in report["results"]] == list(range(6))
    for item in report["results"]:
        assert item["worker_pid"] not in (report["driver_pid"], before["pid"])
        assert item["value"] == expected_base + item["index"]
        assert item["finished_ns"] > item["started_ns"]
        assert len(item["digest"]) == 64


def test_task_failure_is_surfaced_without_success_report() -> None:
    result = run_example("--tasks", "6", "--fail-at", "0")
    assert result.returncode == 1, result.stdout + result.stderr
    assert "Intentional example task failure" in result.stderr
    assert "RayTaskError" in result.stderr
    assert "RAY_DEMO_REPORT" not in result.stdout


def test_batch_deadline_is_surfaced_without_success_report() -> None:
    result = run_example("--timeout-seconds", "0.001")
    assert result.returncode == 1, result.stdout + result.stderr
    assert "DemoTimeout" in result.stderr
    assert "RAY_DEMO_REPORT" not in result.stdout
