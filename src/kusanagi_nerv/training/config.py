"""Inputs and result contract, importable without PyTorch."""

import math
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class TrainingConfig:
    seed: int = 42
    epochs: int = 20
    batch_size: int = 32
    learning_rate: float = 0.01

    def __post_init__(self) -> None:
        for name, lower, upper in (
            ("seed", 0, 2**32 - 1),
            ("epochs", 1, 200),
            ("batch_size", 1, 384),
        ):
            value = getattr(self, name)
            if type(value) is not int or not lower <= value <= upper:
                raise ValueError(f"{name} must be an integer in [{lower}, {upper}]")
        rate = self.learning_rate
        if (
            type(rate) not in (int, float)
            or not 0 < rate <= 1
            or not math.isfinite(rate)
        ):
            raise ValueError("learning_rate must be finite and in (0, 1]")


@dataclass(frozen=True, slots=True)
class TrainingResult:
    artifact_path: Path
    artifact_sha256: str
    completed_epochs: int
    final_train_mse: float
    initial_eval_mse: float
    final_eval_mse: float
    final_eval_mae: float
