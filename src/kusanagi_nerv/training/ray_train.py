"""Run one Ray Train attempt in an owned single-node runtime."""

import argparse
import json
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path

from kusanagi_nerv.runtime.environment import runtime_environment

from .config import TrainingConfig
from .distributed_config import RESULT_PREFIX, DistributedConfig, DistributedResult


def run_local(config: DistributedConfig, output: Path) -> DistributedResult:
    if "ray" in sys.modules:
        raise RuntimeError(
            "Run the local launcher in a fresh process before importing Ray"
        )
    with tempfile.TemporaryDirectory(prefix="kn-ray-ddp-", dir="/tmp") as directory:
        with runtime_environment(directory):
            import ray

            from .distributed import train_distributed

            try:
                ray.init(
                    address="local",
                    num_cpus=config.num_workers,
                    num_gpus=0 if config.device == "cpu" else None,
                    include_dashboard=False,
                    _node_ip_address="127.0.0.1",
                    _temp_dir=directory,
                    object_store_memory=128 * 1024 * 1024,
                    log_to_driver=True,
                )
                return train_distributed(config, output)
            finally:
                ray.shutdown()


def main() -> int:
    defaults = TrainingConfig()
    parser = argparse.ArgumentParser(
        description="Train the tiny regressor with Ray Train v2"
    )
    parser.add_argument("--submission-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--num-workers", type=int, default=2)
    parser.add_argument("--device", choices=("cpu", "cuda"), default="cpu")
    parser.add_argument("--seed", type=int, default=defaults.seed)
    parser.add_argument("--epochs", type=int, default=defaults.epochs)
    parser.add_argument("--batch-size", type=int, default=defaults.batch_size)
    parser.add_argument("--learning-rate", type=float, default=defaults.learning_rate)
    args = parser.parse_args()
    try:
        config = DistributedConfig(
            args.submission_id,
            TrainingConfig(args.seed, args.epochs, args.batch_size, args.learning_rate),
            args.num_workers,
            args.device,
        )
    except ValueError as exc:
        parser.error(str(exc))
    try:
        result = run_local(config, args.output)
    except ImportError as exc:
        print(
            f"Cannot import Ray Train dependencies: {exc}. Run uv sync --locked --extra ray-train",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:
        print(f"Ray Train failed ({type(exc).__name__}): {exc}", file=sys.stderr)
        return 1
    report = asdict(result)
    report["training"]["artifact_path"] = str(result.training.artifact_path)
    print(
        RESULT_PREFIX + json.dumps(report, sort_keys=True, allow_nan=False), flush=True
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
