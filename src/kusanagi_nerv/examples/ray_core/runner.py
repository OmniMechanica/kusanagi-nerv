"""Own a local Ray runtime and submit work through a bounded ObjectRef window."""

import json
import os
import sys
import tempfile
import time
from contextlib import suppress
from dataclasses import asdict
from typing import Any
from uuid import uuid4

from kusanagi_nerv.runtime.environment import runtime_environment

from .model import DemoConfig, peak_concurrency


class DemoTimeout(TimeoutError):
    """The batch exhausted its deadline after local Ray startup."""


def run_batch(config: DemoConfig) -> dict[str, Any]:
    """Run on the runtime owned by this example; return observed measurements."""
    import ray

    from .workers import ResultCollector, cpu_task

    deadline = time.monotonic() + config.timeout_seconds

    def remaining() -> float:
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise DemoTimeout("Ray example batch deadline exceeded")
        return seconds

    def get(ref):
        try:
            return ray.get(ref, timeout=remaining())
        except ray.exceptions.GetTimeoutError as exc:
            raise DemoTimeout("Ray example batch deadline exceeded") from exc

    actor = ResultCollector.remote()
    outstanding = []
    results = []
    next_index = peak_outstanding = backpressure_waits = 0
    try:
        # Wait for the actor to reserve its CPU before any task is submitted.
        before = get(actor.snapshot.remote())
        payload_ref = ray.put(tuple(range(64)))
        after = before
        while next_index < config.tasks or outstanding:
            while (
                next_index < config.tasks and len(outstanding) < config.max_outstanding
            ):
                remaining()
                outstanding.append(
                    cpu_task.remote(
                        next_index, payload_ref, config.work_iterations, config.fail_at
                    )
                )
                next_index += 1
                peak_outstanding = max(peak_outstanding, len(outstanding))
            if next_index < config.tasks:
                backpressure_waits += 1
            ready, _ = ray.wait(outstanding, num_returns=1, timeout=remaining())
            if not ready:
                raise DemoTimeout("Ray example batch deadline exceeded")
            result = get(ready[0])
            after = get(actor.record.remote(result))
            results.append(result)
            outstanding.remove(ready[0])

        return {
            "evidence": "TESTED-CPU",
            "ray_version": ray.__version__,
            "config": asdict(config),
            "driver_pid": os.getpid(),
            "resources": ray.cluster_resources(),
            "actor_before": asdict(before),
            "actor_after": asdict(after),
            "peak_outstanding": peak_outstanding,
            "backpressure_waits": backpressure_waits,
            "max_running": peak_concurrency(results),
            "results": [
                asdict(result) for result in sorted(results, key=lambda r: r.index)
            ],
        }
    finally:
        # Only handles created by this batch. Cleanup must not hide the original error.
        for ref in outstanding:
            with suppress(Exception):
                ray.cancel(ref, force=True)
        with suppress(Exception):
            ray.kill(actor, no_restart=True)


def run_demo(config: DemoConfig) -> dict[str, Any]:
    """CLI-owned runtime. Run each demonstration in a fresh driver process."""
    # Ray caches authentication configuration at import time. Set the isolated
    # environment first; never replace configuration in an existing Ray driver.
    if "ray" in sys.modules:
        raise RuntimeError("Run the example in a fresh process, before importing Ray")
    # A short path avoids macOS Unix-domain socket path limits.
    with tempfile.TemporaryDirectory(prefix="kn-ray-", dir="/tmp") as directory:
        try:
            with runtime_environment(directory):
                import ray

                try:
                    ray.init(
                        address="local",
                        num_cpus=config.workers + 1,
                        num_gpus=0,
                        include_dashboard=False,
                        object_store_memory=128 * 1024 * 1024,
                        namespace=f"kusanagi-example-{uuid4().hex}",
                        _node_ip_address="127.0.0.1",
                        _temp_dir=directory,
                        log_to_driver=True,
                    )
                    report = run_batch(config)
                finally:
                    ray.shutdown()
        finally:
            runtime = sys.modules.get("ray")
            print(
                "RAY_DEMO_CLEANUP "
                + json.dumps(
                    {
                        "runtime_directory": directory,
                        "ray_initialized": runtime is not None
                        and runtime.is_initialized(),
                    }
                ),
                flush=True,
            )
    report["runtime_shutdown"] = not ray.is_initialized()
    return report
