"""Validate inputs before starting processes; measurements need no Ray import."""

import hashlib
import os
import sys
from pathlib import Path
from types import ModuleType

import pytest

from kusanagi_nerv.examples.ray_core.model import (
    DemoConfig,
    TaskResult,
    compute,
    peak_concurrency,
)
from kusanagi_nerv.examples.ray_core.runner import run_demo, runtime_environment


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tasks", 0),
        ("tasks", 1001),
        ("tasks", True),
        ("workers", 0),
        ("workers", 9),
        ("workers", 1.5),
        ("max_outstanding", 0),
        ("max_outstanding", 129),
        ("work_iterations", 0),
        ("work_iterations", 5_000_001),
        ("timeout_seconds", 0),
        ("timeout_seconds", 301),
        ("timeout_seconds", float("nan")),
        ("timeout_seconds", float("inf")),
        ("timeout_seconds", True),
        ("fail_at", -1),
        ("fail_at", 8),
        ("fail_at", False),
    ],
)
def test_invalid_inputs_fail_before_runtime_start(field: str, value: object) -> None:
    with pytest.raises(ValueError, match=field):
        DemoConfig(**{field: value})


def test_window_can_be_smaller_than_cpu_capacity() -> None:
    assert DemoConfig(workers=2, max_outstanding=1).max_outstanding == 1


def test_work_uses_shared_input_and_task_index() -> None:
    value, digest = compute((2, 3), index=4, iterations=2)
    assert value == 17
    expected = hashlib.sha256(hashlib.sha256(hashlib.sha256(b"17").digest()).digest())
    assert digest == expected.hexdigest()


@pytest.mark.parametrize(
    ("intervals", "expected"),
    [([], 0), ([(1, 4), (2, 5), (4, 6)], 2), ([(1, 2), (2, 3)], 1)],
)
def test_peak_uses_overlap_not_total_worker_count(intervals, expected) -> None:
    results = [
        TaskResult(i, 0, "", i, start, end) for i, (start, end) in enumerate(intervals)
    ]
    assert peak_concurrency(results) == expected


def test_runtime_environment_is_private_and_restored_on_error(
    tmp_path: Path, monkeypatch
) -> None:
    # Synthetic values only: never inspect a user's existing token.
    previous = {
        "RAY_AUTH_MODE": "token",
        "RAY_AUTH_TOKEN": "synthetic-test-value",
        "RAY_AUTH_TOKEN_PATH": "/unused/test-path",
    }
    for key, value in previous.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("RAY_USAGE_STATS_ENABLED", raising=False)
    monkeypatch.setenv("RAY_DEDUP_LOGS", "1")
    with pytest.raises(RuntimeError, match="exercise cleanup"):
        with runtime_environment(str(tmp_path)):
            assert "RAY_AUTH_TOKEN" not in os.environ
            token = Path(os.environ["RAY_AUTH_TOKEN_PATH"])
            assert token.parent == tmp_path
            assert token.stat().st_mode & 0o777 == 0o600
            assert os.environ["RAY_AUTH_MODE"] == "token"
            assert os.environ["RAY_USAGE_STATS_ENABLED"] == "0"
            assert os.environ["RAY_DEDUP_LOGS"] == "0"
            raise RuntimeError("exercise cleanup")
    assert {key: os.environ[key] for key in previous} == previous
    assert "RAY_USAGE_STATS_ENABLED" not in os.environ
    assert os.environ["RAY_DEDUP_LOGS"] == "1"


def test_existing_ray_driver_is_rejected_without_runtime_calls(monkeypatch) -> None:
    # No runtime methods exist on this sentinel: touching it would fail the test.
    existing = ModuleType("ray")
    monkeypatch.setitem(sys.modules, "ray", existing)
    with pytest.raises(RuntimeError, match="fresh process"):
        run_demo(DemoConfig())
    assert sys.modules["ray"] is existing
