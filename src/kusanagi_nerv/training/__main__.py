"""Run an independent CPU training process and print a JSON result."""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from .config import TrainingConfig


def main() -> int:
    defaults = TrainingConfig()
    parser = argparse.ArgumentParser(
        description="Train a tiny PyTorch regressor on CPU"
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="New local artifact path"
    )
    parser.add_argument("--seed", type=int, default=defaults.seed)
    parser.add_argument("--epochs", type=int, default=defaults.epochs)
    parser.add_argument("--batch-size", type=int, default=defaults.batch_size)
    parser.add_argument("--learning-rate", type=float, default=defaults.learning_rate)
    args = parser.parse_args()
    try:
        config = TrainingConfig(
            args.seed, args.epochs, args.batch_size, args.learning_rate
        )
    except ValueError as exc:
        parser.error(str(exc))

    try:
        import torch

        from .loop import train
    except ModuleNotFoundError as exc:
        if exc.name != "torch":
            raise
        print("Install PyTorch with uv sync --locked --extra training", file=sys.stderr)
        return 1

    torch.set_num_threads(1)
    torch.use_deterministic_algorithms(True)
    try:
        result = train(config, args.output)
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Training failed ({type(exc).__name__}): {exc}", file=sys.stderr)
        return 1
    report = asdict(result)
    report["artifact_path"] = str(result.artifact_path)
    print(json.dumps(report, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
