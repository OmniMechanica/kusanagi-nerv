"""Run the existing tiny regressor with Ray Train v2 and PyTorch DDP."""

import hashlib
import json
import math
import os
import shutil
import signal
import tempfile
from dataclasses import asdict
from pathlib import Path

import ray
import torch
from ray import train
from ray.train.torch import TorchConfig, TorchTrainer, get_device, prepare_model
from torch import distributed as dist
from torch.nn.parallel import DistributedDataParallel

from .config import TrainingConfig, TrainingResult
from .distributed_config import (
    START_PREFIX,
    DistributedConfig,
    DistributedResult,
    WorkerEvidence,
)
from .loop import (
    build_model,
    evaluate,
    model_artifact,
    publish_artifact,
    synthetic_split,
    train_epoch,
)
from .recovery_config import CHECKPOINT_PREFIX, RecoveryConfig

WEIGHTS = "model.pt"


def _parameter_sha256(model: torch.nn.Module) -> str:
    digest = hashlib.sha256()
    for parameter in model.parameters():
        digest.update(parameter.detach().cpu().contiguous().numpy().tobytes())
    return digest.hexdigest()


def _worker_loop(values: dict) -> None:
    recovery_payload = values.pop("recovery", None)
    config = DistributedConfig(
        **{**values, "training": TrainingConfig(**values["training"])}
    )
    context = train.get_context()
    rank = context.get_world_rank()
    world_size = context.get_world_size()
    device = get_device()
    if world_size != config.num_workers or device.type != config.device:
        raise RuntimeError(
            "Ray allocated a different worker group or device than requested"
        )
    torch.set_num_threads(1)
    if device.type == "cpu":
        torch.use_deterministic_algorithms(True)
    spec = config.training
    train_x, train_y, eval_x, eval_y = synthetic_split()
    if len(train_x) % world_size:
        raise ValueError("Training rows must divide evenly across DDP workers")
    shard_size = len(train_x) // world_size
    model = prepare_model(build_model(spec.seed), parallel_strategy="ddp")
    if not isinstance(model, DistributedDataParallel):
        raise RuntimeError("Expected PyTorch DistributedDataParallel")
    optimizer = torch.optim.Adam(
        model.parameters(), lr=spec.learning_rate, foreach=False
    )
    rng = torch.Generator(device="cpu").manual_seed(spec.seed + 1)
    local_order = torch.arange(shard_size, device=device)
    history = []
    if rank == 0:
        eval_x, eval_y = eval_x.to(device), eval_y.to(device)
        initial_mse, initial_mae = evaluate(model.module, eval_x, eval_y)

    first_epoch, restored_step = 1, 0
    recovery = None
    if recovery_payload is not None:
        from .checkpoint import publish_checkpoint, restore_checkpoint
        from .object_store import ObjectStore, ObjectStoreSettings

        recovery = RecoveryConfig.from_payload(recovery_payload)
        store = ObjectStore(ObjectStoreSettings.from_environment())
        if store.settings.identity != recovery.storage_identity:
            raise ValueError("Recovery object store identity changed")
        last_epoch, restored_step, history = restore_checkpoint(
            store, recovery, config, model.module, optimizer, rng
        )
        first_epoch = last_epoch + 1

    for epoch in range(first_epoch, spec.epochs + 1):
        permutation = torch.randperm(len(train_x), generator=rng, device="cpu")
        indices = permutation[rank::world_size]
        features, targets = train_x[indices].to(device), train_y[indices].to(device)
        if epoch == first_epoch:
            # Wait until all ranks have the model and the first shard. The
            # event is attributable to this submission, even in Ray Jobs logs.
            dist.barrier()
            if rank == 0:
                print(
                    START_PREFIX
                    + json.dumps(
                        {
                            "submission_id": config.submission_id,
                            "world_size": world_size,
                            "device": device.type,
                            "rank": rank,
                            "worker_pid": os.getpid(),
                            "first_epoch": first_epoch,
                            "restored_optimizer_step": restored_step,
                        }
                    ),
                    flush=True,
                )
        mse = train_epoch(
            model, optimizer, features, targets, local_order, spec.batch_size
        )
        summed_mse = torch.tensor(mse, dtype=torch.float64, device=device)
        dist.all_reduce(summed_mse)
        train_mse = summed_mse.item() / world_size  # Equal-size disjoint shards.
        if not math.isfinite(train_mse):
            raise ValueError(f"Non-finite training metric at epoch {epoch}")
        if rank == 0:
            eval_mse, eval_mae = evaluate(model.module, eval_x, eval_y)
            if not all(map(math.isfinite, (eval_mse, eval_mae))):
                raise ValueError(f"Non-finite evaluation metric at epoch {epoch}")
            history.append(
                {
                    "epoch": epoch,
                    "train_mse": train_mse,
                    "eval_mse": eval_mse,
                    "eval_mae": eval_mae,
                }
            )
        if recovery is not None:
            if rank == 0:
                checkpoint = publish_checkpoint(
                    store,
                    recovery,
                    config,
                    epoch,
                    model.module,
                    optimizer,
                    rng,
                    history,
                )
                print(
                    CHECKPOINT_PREFIX
                    + json.dumps(
                        {"submission_id": config.submission_id, **asdict(checkpoint)}
                    ),
                    flush=True,
                )
                if recovery.kill_after_epoch == epoch:
                    os.kill(os.getpid(), signal.SIGKILL)
            dist.barrier()

    worker = WorkerEvidence(
        rank,
        context.get_local_rank(),
        os.getpid(),
        str(device),
        shard_size,
        math.ceil(shard_size / spec.batch_size),
        _parameter_sha256(model.module),
    )
    workers = [None] * world_size
    dist.all_gather_object(workers, asdict(worker))
    if rank != 0:
        train.report({})
        return

    metrics = {
        "workers": workers,
        "backend": dist.get_backend(),
        "completed_epochs": spec.epochs,
        "initial_eval_mse": initial_mse,
        "final_train_mse": train_mse,
        "final_eval_mse": eval_mse,
        "final_eval_mae": eval_mae,
    }
    artifact = model_artifact(model.module, spec, initial_mse, initial_mae, history)
    artifact["environment"]["device"] = device.type
    artifact["distributed"] = {
        "backend": metrics["backend"],
        "world_size": world_size,
        "submission_id": config.submission_id,
        "workers": workers,
        "first_epoch": first_epoch,
        "restored_optimizer_step": restored_step,
    }
    with tempfile.TemporaryDirectory(prefix="kn-train-model-") as directory:
        torch.save(artifact, Path(directory) / WEIGHTS)
        train.report(metrics, checkpoint=train.Checkpoint.from_directory(directory))


def train_distributed(
    config: DistributedConfig, output: Path, *, recovery: RecoveryConfig | None = None
) -> DistributedResult:
    """Run on an initialized Ray cluster with a local shared filesystem."""
    if not ray.is_initialized():
        raise RuntimeError("Initialize the Ray runtime before starting Ray Train")
    if not hasattr(train.FailureConfig, "max_preemption_failures"):
        raise RuntimeError("Ray Train v2 is required")
    output = output.parent.resolve() / output.name
    if os.path.lexists(output):
        raise FileExistsError(f"Output already exists: {output}")
    use_gpu = config.device == "cuda"
    with tempfile.TemporaryDirectory(prefix="kn-ray-train-storage-") as storage:
        result = TorchTrainer(
            _worker_loop,
            train_loop_config={
                **asdict(config),
                "recovery": asdict(recovery) if recovery else None,
            },
            scaling_config=train.ScalingConfig(
                num_workers=config.num_workers,
                use_gpu=use_gpu,
                resources_per_worker={"CPU": 1, "GPU": int(use_gpu)},
            ),
            torch_config=TorchConfig(
                backend="nccl" if use_gpu else "gloo", timeout_s=60
            ),
            run_config=train.RunConfig(
                name=config.submission_id,
                storage_path=storage,
                failure_config=train.FailureConfig(
                    max_failures=0,
                    max_preemption_failures=0,
                    controller_failure_limit=0,
                ),
            ),
        ).fit()
        if result.error is not None:
            raise RuntimeError("Ray Train attempt failed") from result.error
        if result.checkpoint is None or result.metrics is None:
            raise RuntimeError("Ray Train finished without a model or metrics")
        metrics = result.metrics
        workers = tuple(WorkerEvidence(**record) for record in metrics["workers"])
        if (
            tuple(w.rank for w in workers) != tuple(range(config.num_workers))
            or len({w.parameter_sha256 for w in workers}) != 1
        ):
            raise RuntimeError("Missing workers or divergent DDP parameters")
        with (
            result.checkpoint.as_directory() as directory,
            (Path(directory) / WEIGHTS).open("rb") as source,
        ):
            sha256 = publish_artifact(
                output, lambda destination: shutil.copyfileobj(source, destination)
            )
    training = TrainingResult(
        output,
        sha256,
        metrics["completed_epochs"],
        metrics["final_train_mse"],
        metrics["initial_eval_mse"],
        metrics["final_eval_mse"],
        metrics["final_eval_mae"],
    )
    return DistributedResult(
        config.submission_id,
        os.getpid(),
        config.num_workers,
        metrics["backend"],
        workers,
        training,
    )
