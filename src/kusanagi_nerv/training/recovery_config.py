"""Versioned recovery wire contract shared by the Ray Jobs driver and adapter."""

from dataclasses import dataclass
from typing import Self

from kusanagi_nerv.domain.execution import CheckpointRef

INPUT_ENV = "KUSANAGI_RECOVERY_INPUT"
CHECKPOINT_PREFIX = "KUSANAGI_CHECKPOINT_PUBLISHED "
RECOVERY_RESULT_PREFIX = "KUSANAGI_RECOVERY_RESULT "
WORKER_LOST_EXIT = 75


@dataclass(frozen=True, slots=True)
class RecoveryConfig:
    job_id: str
    storage_identity: str
    resume: CheckpointRef | None = None
    kill_after_epoch: int | None = None

    @classmethod
    def from_payload(cls, payload: dict) -> Self:
        resume = payload.get("resume")
        return cls(**{**payload, "resume": CheckpointRef(**resume) if resume else None})
