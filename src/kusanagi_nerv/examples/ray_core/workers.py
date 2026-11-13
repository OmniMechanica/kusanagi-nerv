"""Remote task and a serial, stateful actor with explicit CPU reservations."""

import json
import os
import time

import ray

from .model import ActorSnapshot, TaskResult, compute


@ray.remote(num_cpus=1, num_gpus=0, max_retries=0, retry_exceptions=False)
def cpu_task(
    index: int, payload: tuple[int, ...], iterations: int, fail_at: int | None
) -> TaskResult:
    started = time.monotonic_ns()
    pid = os.getpid()
    print(
        json.dumps(
            {"event": "task_start", "index": index, "pid": pid, "monotonic_ns": started}
        ),
        flush=True,
    )
    try:
        if index == fail_at:
            raise ValueError("Intentional example task failure")
        value, digest = compute(payload, index, iterations)
    except Exception:
        print(
            json.dumps({"event": "task_error", "index": index, "pid": pid}), flush=True
        )
        raise
    finished = time.monotonic_ns()
    print(
        json.dumps(
            {"event": "task_end", "index": index, "pid": pid, "monotonic_ns": finished}
        ),
        flush=True,
    )
    return TaskResult(index, value, digest, pid, started, finished)


@ray.remote(
    num_cpus=1, num_gpus=0, max_restarts=0, max_task_retries=0, max_concurrency=1
)
class ResultCollector:
    """Retain state between calls in one actor process; no persistence/replay."""

    def __init__(self) -> None:
        self._completed: list[int] = []
        self._value_sum = 0

    def record(self, result: TaskResult) -> ActorSnapshot:
        if result.index in self._completed:
            raise ValueError(f"Duplicate result: {result.index}")
        self._completed.append(result.index)
        self._value_sum += result.value
        return self.snapshot()

    def snapshot(self) -> ActorSnapshot:
        return ActorSnapshot(
            os.getpid(), len(self._completed), self._value_sum, tuple(self._completed)
        )
