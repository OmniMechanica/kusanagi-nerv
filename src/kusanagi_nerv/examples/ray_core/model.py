"""Pure Python inputs, results and measurements for the Ray example."""

import hashlib
import math
from dataclasses import dataclass


@dataclass(frozen=True)
class DemoConfig:
    tasks: int = 8
    workers: int = 2
    max_outstanding: int = 4
    work_iterations: int = 250_000
    timeout_seconds: float = 60
    fail_at: int | None = None

    def __post_init__(self) -> None:
        for name, upper in (
            ("tasks", 1000),
            ("workers", 8),
            ("max_outstanding", 128),
            ("work_iterations", 5_000_000),
        ):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= upper:
                raise ValueError(f"{name} must be an integer in [1, {upper}]")
        if (
            type(self.timeout_seconds) not in (int, float)
            or not math.isfinite(self.timeout_seconds)
            or not 0 < self.timeout_seconds <= 300
        ):
            raise ValueError("timeout_seconds must be finite and in (0, 300]")
        if self.fail_at is not None and (
            type(self.fail_at) is not int or not 0 <= self.fail_at < self.tasks
        ):
            raise ValueError("fail_at must be a task index in [0, tasks)")


@dataclass(frozen=True)
class TaskResult:
    index: int
    value: int
    digest: str
    worker_pid: int
    started_ns: int
    finished_ns: int


@dataclass(frozen=True)
class ActorSnapshot:
    pid: int
    completed: int
    value_sum: int
    completion_order: tuple[int, ...]


def compute(payload: tuple[int, ...], index: int, iterations: int) -> tuple[int, str]:
    """Do deterministic CPU work, without sleeping or calling Ray."""
    value = sum(item * item for item in payload) + index
    digest = hashlib.sha256(str(value).encode()).digest()
    for _ in range(iterations):
        digest = hashlib.sha256(digest).digest()
    return value, digest.hex()


def peak_concurrency(results: list[TaskResult]) -> int:
    """Measure overlapping task bodies on this single node's monotonic clock."""
    events = [
        event
        for result in results
        for event in ((result.started_ns, 1), (result.finished_ns, -1))
    ]
    running = peak = 0
    for _, change in sorted(events):  # End before start at the same instant.
        running += change
        peak = max(peak, running)
    return peak
