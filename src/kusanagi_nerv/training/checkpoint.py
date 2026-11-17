"""Persist model, optimizer and data-order state between controller attempts."""

import io
import re
from dataclasses import asdict

import torch

from kusanagi_nerv.domain.execution import CheckpointRef

from .distributed_config import DistributedConfig
from .loop import DATASET, MODEL
from .object_store import ObjectStore, attempt_prefix, checkpoint_key
from .recovery_config import RecoveryConfig


def compatibility(config: DistributedConfig) -> dict:
    return {
        "model": MODEL,
        "dataset": DATASET,
        "training": asdict(config.training),
        "num_workers": config.num_workers,
        "device": config.device,
        "torch": str(torch.__version__),
    }


def publish_checkpoint(
    store: ObjectStore,
    recovery: RecoveryConfig,
    config: DistributedConfig,
    epoch: int,
    model,
    optimizer,
    rng,
    history: list[dict],
) -> CheckpointRef:
    prefix = attempt_prefix(recovery.job_id, config.submission_id)
    stream = io.BytesIO()
    torch.save(
        {
            "format_version": 1,
            "job_id": recovery.job_id,
            "submission_id": config.submission_id,
            "epoch": epoch,
            "compatibility": compatibility(config),
            "model": model.state_dict(),
            "optimizer": optimizer.state_dict(),
            "batch_rng": rng.get_state(),
            "history": history,
        },
        stream,
    )
    uri, digest = store.publish(checkpoint_key(prefix, epoch), stream.getvalue())
    return CheckpointRef(uri, digest, epoch)


def restore_checkpoint(
    store: ObjectStore,
    recovery: RecoveryConfig,
    config: DistributedConfig,
    model,
    optimizer,
    rng,
) -> tuple[int, int, list[dict]]:
    reference = recovery.resume
    if reference is None:
        return 0, 0, []
    root = store.uri(f"jobs/{recovery.job_id}/attempts/")
    match = re.fullmatch(
        r"(kusanagi-[0-9a-f-]{36})/checkpoints/(\d{4})\.pt",
        reference.uri.removeprefix(root),
    )
    if (
        not reference.uri.startswith(root)
        or not match
        or match[1] == config.submission_id
        or int(match[2]) != reference.epoch
        or not 1 <= reference.epoch < config.training.epochs
    ):
        raise ValueError(
            "Resume checkpoint does not belong to an earlier attempt of this job"
        )
    data = store.read(reference.uri, reference.sha256)
    state = torch.load(io.BytesIO(data), weights_only=True, map_location="cpu")
    if (
        state.get("format_version") != 1
        or state.get("job_id") != recovery.job_id
        or state.get("submission_id") != match[1]
        or state.get("epoch") != reference.epoch
        or state.get("compatibility") != compatibility(config)
        or len(state.get("history", [])) != reference.epoch
    ):
        raise ValueError("Resume checkpoint metadata is incompatible")
    model.load_state_dict(state["model"], strict=True)
    optimizer.load_state_dict(state["optimizer"])
    rng.set_state(state["batch_rng"])
    steps = {int(record["step"].item()) for record in optimizer.state.values()}
    if len(steps) != 1 or min(steps) <= 0:
        raise ValueError("Resume checkpoint has inconsistent optimizer steps")
    return reference.epoch, steps.pop(), state["history"]
