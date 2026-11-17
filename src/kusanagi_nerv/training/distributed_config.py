"""Serializable inputs and outputs for a single Ray Train attempt."""

import re
from dataclasses import dataclass
from typing import Literal

from .config import TrainingConfig, TrainingResult

START_PREFIX = "KUSANAGI_TRAINING_STARTED "
RESULT_PREFIX = "KUSANAGI_TRAINING_RESULT "


@dataclass(frozen=True, slots=True)
class DistributedConfig:
    submission_id: str
    training: TrainingConfig = TrainingConfig()
    num_workers: int = 2
    device: Literal["cpu", "cuda"] = "cpu"

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", self.submission_id):
            raise ValueError(
                "submission_id must be a 1–64 character Ray submission identifier"
            )
        if type(self.num_workers) is not int or self.num_workers not in (2, 3, 4):
            raise ValueError("num_workers must be 2, 3 or 4")
        if self.device not in ("cpu", "cuda"):
            raise ValueError("device must be cpu or cuda")


@dataclass(frozen=True, slots=True)
class WorkerEvidence:
    rank: int
    local_rank: int
    pid: int
    device: str
    rows_per_epoch: int
    steps_per_epoch: int
    parameter_sha256: str


@dataclass(frozen=True, slots=True)
class DistributedResult:
    submission_id: str
    driver_pid: int
    world_size: int
    backend: str
    workers: tuple[WorkerEvidence, ...]
    training: TrainingResult
