"""Train through the CLI and verify the artifact from another process."""

import hashlib
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest

from kusanagi_nerv.training.config import TrainingConfig

pytestmark = pytest.mark.training
torch = pytest.importorskip("torch")


def test_concurrent_model_initialization_uses_independent_rngs(monkeypatch) -> None:
    from kusanagi_nerv.training.loop import build_model

    seeds = (42, 43, 42, 43)
    reference = {seed: build_model(seed).state_dict() for seed in set(seeds)}
    barrier = Barrier(len(seeds), timeout=10)
    reset_parameters = torch.nn.Linear.reset_parameters

    def synchronize_reset(layer):
        # Bring all workers to the same layer before any of them draws weights.
        barrier.wait()
        reset_parameters(layer)

    monkeypatch.setattr(torch.nn.Linear, "reset_parameters", synchronize_reset)
    rng_before = torch.get_rng_state()
    try:
        with ThreadPoolExecutor(max_workers=len(seeds)) as pool:
            models = list(pool.map(build_model, seeds))
        rng_after = torch.get_rng_state()
    finally:
        torch.set_rng_state(rng_before)

    for seed, model in zip(seeds, models, strict=True):
        for name, weight in model.state_dict().items():
            assert torch.equal(weight, reference[seed][name]), (seed, name)
    assert torch.equal(rng_before, rng_after)


def run_training(
    directory: Path, output: Path, seed: int
) -> subprocess.CompletedProcess[str]:
    # Running outside the checkout also exercises the installed module entrypoint.
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-m",
            "kusanagi_nerv.training",
            "--output",
            str(output),
            "--seed",
            str(seed),
            "--batch-size",
            "31",
        ],
        cwd=directory,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


def test_cpu_training_is_reproducible_and_loads_for_inference(tmp_path: Path) -> None:
    from kusanagi_nerv.training.loop import load_model, synthetic_split

    reports = []
    artifacts = []
    for directory, seed in (("first", 42), ("repeat", 42), ("other-seed", 43)):
        artifact_path = tmp_path / directory / "weights.pt"
        process = run_training(tmp_path, artifact_path.relative_to(tmp_path), seed)
        assert process.returncode == 0, process.stderr
        report = json.loads(process.stdout)
        assert report["artifact_path"] == str(artifact_path.resolve())
        with artifact_path.open("rb") as stream:
            assert (
                report["artifact_sha256"]
                == hashlib.file_digest(stream, "sha256").hexdigest()
            )
        artifacts.append(
            torch.load(artifact_path, map_location="cpu", weights_only=True)
        )
        reports.append(report)

    first, second, different = artifacts
    assert first["metrics"] == second["metrics"]
    assert first["dataset"] == second["dataset"] == different["dataset"]
    assert first["config"] == {
        "seed": 42,
        "epochs": 20,
        "batch_size": 31,
        "learning_rate": 0.01,
    }
    assert first["environment"]["device"] == "cpu"
    assert first["environment"]["torch_threads"] == 1
    assert first["environment"]["deterministic_algorithms"] is True
    for key, value in first["model_state_dict"].items():
        assert value.device.type == "cpu"
        assert torch.equal(value, second["model_state_dict"][key])
    assert any(
        not torch.equal(value, different["model_state_dict"][key])
        for key, value in first["model_state_dict"].items()
    )

    train_x, train_y, eval_x, eval_y = synthetic_split()
    assert (len(train_x), len(eval_x)) == (384, 128)
    assert not (
        {tuple(row) for row in train_x.tolist()}
        & {tuple(row) for row in eval_x.tolist()}
    )
    model = load_model(Path(reports[0]["artifact_path"]))
    assert not model.training
    with torch.inference_mode():
        prediction = model(eval_x)
    assert prediction.shape == eval_y.shape
    assert torch.isfinite(prediction).all()
    error = prediction - eval_y
    report = reports[0]
    assert report["completed_epochs"] == 20
    assert report["final_eval_mae"] == pytest.approx(
        error.abs().mean().item(), abs=1e-6
    )
    assert report["final_eval_mse"] == pytest.approx(
        error.square().mean().item(), abs=1e-6
    )
    constant_mse = (train_y.mean() - eval_y).square().mean().item()
    assert report["final_eval_mse"] < 0.1 * min(
        report["initial_eval_mse"], constant_mse
    )


def test_cli_does_not_replace_existing_output(tmp_path: Path) -> None:
    path = tmp_path / "weights.pt"
    path.write_bytes(b"existing artifact")
    process = run_training(tmp_path, path, 42)
    assert process.returncode == 1
    assert "Output already exists" in process.stderr
    assert not process.stdout
    assert path.read_bytes() == b"existing artifact"


def test_failed_write_does_not_publish_partial_weights(
    tmp_path: Path, monkeypatch
) -> None:
    from kusanagi_nerv.training.loop import train

    def fail_save(artifact, handle):
        handle.write(b"partial artifact")
        raise OSError("injected storage error")

    monkeypatch.setattr(torch, "save", fail_save)
    with pytest.raises(OSError, match="injected storage error"):
        train(TrainingConfig(epochs=1, batch_size=384), tmp_path / "weights.pt")
    assert not list(tmp_path.iterdir())
