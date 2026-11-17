"""Train with real Ray workers and verify the machine-readable attempt contract."""

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from kusanagi_nerv.training.config import TrainingConfig
from kusanagi_nerv.training.distributed_config import RESULT_PREFIX, START_PREFIX

pytestmark = [
    pytest.mark.ray_train,
    pytest.mark.skipif(
        any(
            importlib.util.find_spec(name) is None
            for name in ("ray", "torch", "pyarrow", "pandas")
        ),
        reason="Install the ray-train extra",
    ),
]


def run_attempt(
    output: Path, submission_id: str, *args: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-m",
            "kusanagi_nerv.training.ray_train",
            "--submission-id",
            submission_id,
            "--output",
            str(output),
            *args,
        ],
        cwd=output.parent,
        capture_output=True,
        text=True,
        timeout=150,
        check=False,
        env={
            **os.environ,
            "CUDA_VISIBLE_DEVICES": "",
            "RAY_TRAIN_V2_ENABLED": "1",
            "RAY_TRAIN_WORKER_GROUP_START_TIMEOUT_S": "15",
        },
    )


def records(output: str, prefix: str) -> list[dict]:
    return [
        json.loads(line.split(prefix, 1)[1])
        for line in output.splitlines()
        if prefix in line
    ]


def test_two_real_workers_produce_repeatable_weights_and_metrics(
    tmp_path: Path,
) -> None:
    import torch

    from kusanagi_nerv.training.loop import load_model, synthetic_split, train

    reports = []
    predictions = []
    for submission_id in ("ddp-first", "ddp-repeat"):
        output = tmp_path / f"{submission_id}.pt"
        process = run_attempt(output, submission_id)
        logs = process.stdout + process.stderr
        assert process.returncode == 0, logs
        (report,) = records(process.stdout, RESULT_PREFIX)
        starts = records(logs, START_PREFIX)
        assert report["submission_id"] == submission_id
        assert report["world_size"] == 2 and report["backend"] == "gloo"
        workers = report["workers"]
        assert [(worker["rank"], worker["local_rank"]) for worker in workers] == [
            (0, 0),
            (1, 1),
        ]
        assert len({report["driver_pid"], *(worker["pid"] for worker in workers)}) == 3
        assert all(worker["device"] == "cpu" for worker in workers)
        assert sum(worker["rows_per_epoch"] for worker in workers) == 384
        assert [worker["steps_per_epoch"] for worker in workers] == [6, 6]
        assert workers[0]["parameter_sha256"] == workers[1]["parameter_sha256"]
        assert starts and all(
            event
            == {
                "submission_id": submission_id,
                "world_size": 2,
                "device": "cpu",
                "rank": 0,
                "worker_pid": workers[0]["pid"],
            }
            for event in starts
        )

        training = report["training"]
        assert training["artifact_path"] == str(output.resolve())
        assert (
            training["artifact_sha256"]
            == hashlib.sha256(output.read_bytes()).hexdigest()
        )
        assert training["completed_epochs"] == 20
        assert training["final_eval_mse"] < 0.1 * training["initial_eval_mse"]
        _, _, eval_x, eval_y = synthetic_split()
        with torch.inference_mode():
            prediction = load_model(output)(eval_x)
        assert (prediction - eval_y).square().mean().item() == pytest.approx(
            training["final_eval_mse"], abs=1e-6
        )
        reports.append(report)
        predictions.append(prediction)

    assert torch.equal(*predictions)
    assert (
        reports[0]["workers"][0]["parameter_sha256"]
        == reports[1]["workers"][0]["parameter_sha256"]
    )
    for key in ("final_train_mse", "final_eval_mse", "final_eval_mae"):
        assert reports[0]["training"][key] == reports[1]["training"][key]

    # One serial step over the union of the two shards is the same update
    # as one DDP step, up to floating-point reduction order.
    reference = tmp_path / "serial-reference.pt"
    train(TrainingConfig(batch_size=64), reference)
    with torch.inference_mode():
        serial_prediction = load_model(reference)(eval_x)
    torch.testing.assert_close(predictions[0], serial_prediction, rtol=1e-4, atol=1e-5)


def test_gpu_placement_without_gpus_does_not_emit_training_start(
    tmp_path: Path,
) -> None:
    output = tmp_path / "gpu-no-capacity.pt"
    process = run_attempt(output, "gpu-no-capacity", "--device", "cuda")
    logs = process.stdout + process.stderr
    assert process.returncode == 1, logs
    assert "Attempting to start training worker group" in logs and "'GPU': 1" in logs
    assert "Ray Train failed" in logs
    assert START_PREFIX not in logs and RESULT_PREFIX not in logs
    assert not output.exists()
