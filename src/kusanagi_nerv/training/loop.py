"""Train a tiny regressor and publish one local weights-and-metadata artifact."""

import hashlib
import math
import os
import platform
import tempfile
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import BinaryIO

import torch
from torch import Tensor, nn

from .config import TrainingConfig, TrainingResult

DATASET = "synthetic-v1"
MODEL = "tiny-mlp-v1"
ARTIFACT_VERSION = 1
DATA_SEED = 1234
SPLIT_SEED = 4321
TRAIN_ROWS = 384
EVAL_ROWS = 128


def synthetic_split() -> tuple[Tensor, Tensor, Tensor, Tensor]:
    """Return disjoint train/eval tensors. The dataset version fixes both seeds."""
    data_rng = torch.Generator(device="cpu").manual_seed(DATA_SEED)
    features = torch.randn(
        TRAIN_ROWS + EVAL_ROWS, 4, generator=data_rng, device="cpu", dtype=torch.float32
    )
    noise = torch.randn(
        len(features), generator=data_rng, device="cpu", dtype=torch.float32
    )
    a, b, c, d = features.unbind(dim=1)
    targets = 1.5 * a - 2 * b + 0.75 * c * d + 0.05 * noise
    split_rng = torch.Generator(device="cpu").manual_seed(SPLIT_SEED)
    order = torch.randperm(len(features), generator=split_rng, device="cpu")
    train, evaluation = order[:TRAIN_ROWS], order[TRAIN_ROWS:]
    return features[train], targets[train], features[evaluation], targets[evaluation]


def _seeded_linear(inputs: int, outputs: int, generator: torch.Generator) -> nn.Linear:
    # Allocate without drawing from the default CPU RNG, then initialize both
    # tensors with Linear's usual bounds and the caller's private generator.
    layer = nn.Linear(inputs, outputs, device="meta", dtype=torch.float32)
    layer.to_empty(device="cpu")
    nn.init.kaiming_uniform_(layer.weight, a=math.sqrt(5), generator=generator)
    if layer.bias is not None:
        bound = 1 / math.sqrt(inputs)
        nn.init.uniform_(layer.bias, -bound, bound, generator=generator)
    return layer


class TinyRegressor(nn.Module):
    def __init__(self, seed: int = 0) -> None:
        super().__init__()
        generator = torch.Generator(device="cpu").manual_seed(seed)
        self.layers = nn.Sequential(
            _seeded_linear(4, 24, generator),
            nn.ReLU(),
            _seeded_linear(24, 1, generator),
        )

    def forward(self, features: Tensor) -> Tensor:
        return self.layers(features).squeeze(-1)


def build_model(seed: int) -> TinyRegressor:
    return TinyRegressor(seed)


def train_epoch(
    model: nn.Module,
    optimizer: torch.optim.Optimizer,
    features: Tensor,
    targets: Tensor,
    order: Tensor,
    batch_size: int,
) -> float:
    model.train()
    squared_error = 0.0
    for indices in order.split(batch_size):
        optimizer.zero_grad(set_to_none=True)
        prediction = model(features[indices])
        loss = nn.functional.mse_loss(prediction, targets[indices])
        loss.backward()
        optimizer.step()
        squared_error += loss.detach().item() * len(indices)
    return squared_error / len(features)


@torch.inference_mode()
def evaluate(
    model: nn.Module, features: Tensor, targets: Tensor
) -> tuple[float, float]:
    model.eval()
    error = model(features) - targets
    return error.square().mean().item(), error.abs().mean().item()


def train(config: TrainingConfig, output: Path) -> TrainingResult:
    """Run CPU training and publish a complete artifact at a new output path."""
    output = output.parent.resolve() / output.name
    if os.path.lexists(output):
        raise FileExistsError(f"Output already exists: {output}")
    train_x, train_y, eval_x, eval_y = synthetic_split()
    model = build_model(config.seed)
    optimizer = torch.optim.Adam(
        model.parameters(), lr=config.learning_rate, foreach=False
    )
    batch_rng = torch.Generator(device="cpu").manual_seed(config.seed + 1)

    initial_mse, initial_mae = evaluate(model, eval_x, eval_y)
    history = []
    for epoch in range(1, config.epochs + 1):
        order = torch.randperm(len(train_x), generator=batch_rng, device="cpu")
        train_mse = train_epoch(
            model, optimizer, train_x, train_y, order, config.batch_size
        )
        eval_mse, eval_mae = evaluate(model, eval_x, eval_y)
        if not all(map(math.isfinite, (train_mse, eval_mse, eval_mae))):
            raise ValueError(f"Non-finite training metric at epoch {epoch}")
        history.append(
            {
                "epoch": epoch,
                "train_mse": train_mse,
                "eval_mse": eval_mse,
                "eval_mae": eval_mae,
            }
        )

    artifact = model_artifact(model, config, initial_mse, initial_mae, history)
    sha256 = publish_artifact(output, lambda stream: torch.save(artifact, stream))
    return TrainingResult(
        output, sha256, config.epochs, train_mse, initial_mse, eval_mse, eval_mae
    )


def model_artifact(
    model: nn.Module,
    config: TrainingConfig,
    initial_mse: float,
    initial_mae: float,
    history: list[dict[str, int | float]],
) -> dict:
    return {
        "format_version": ARTIFACT_VERSION,
        "model": MODEL,
        "dataset": {
            "name": DATASET,
            "data_seed": DATA_SEED,
            "split_seed": SPLIT_SEED,
            "train_rows": TRAIN_ROWS,
            "eval_rows": EVAL_ROWS,
        },
        "config": asdict(config),
        "environment": {
            "device": "cpu",
            "dtype": "float32",
            "torch": str(torch.__version__),
            "python": platform.python_version(),
            "system": platform.system(),
            "architecture": platform.machine(),
            "torch_threads": torch.get_num_threads(),
            "deterministic_algorithms": torch.are_deterministic_algorithms_enabled(),
        },
        "metrics": {
            "initial_eval_mse": initial_mse,
            "initial_eval_mae": initial_mae,
            "epochs": history,
        },
        "model_state_dict": model.state_dict(),
    }


def publish_artifact(output: Path, write: Callable[[BinaryIO], object]) -> str:
    # Stage in the destination directory. Linking the complete file publishes
    # atomically and fails if another run already claimed the output path.
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=output.parent, prefix=".kn-weights-"
    ) as staged:
        write(staged)
        staged.flush()
        staged.seek(0)
        sha256 = hashlib.file_digest(staged, "sha256").hexdigest()
        os.link(staged.name, output)
    return sha256


def load_model(path: Path) -> nn.Module:
    """Restore the weights for CPU inference from a trusted local artifact."""
    artifact = torch.load(path, map_location="cpu", weights_only=True)
    if (
        not isinstance(artifact, dict)
        or artifact.get("format_version") != ARTIFACT_VERSION
        or artifact.get("model") != MODEL
    ):
        raise ValueError("Unsupported training artifact format or model")
    model = build_model(seed=0)
    model.load_state_dict(artifact["model_state_dict"], strict=True)
    return model.eval()
