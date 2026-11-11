"""Runtime-independent dispatch identity, observations and lease authority."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from kusanagi_nerv.domain.jobs import Job


class ExecutionStatus(StrEnum):
    NOT_FOUND = "not_found"
    UNKNOWN = "unknown"
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class Claim:
    job: Job
    attempt_id: UUID
    submission_id: str
    owner: str
    epoch: int
    accepted: bool


class LeaseLost(RuntimeError):
    """This claim no longer authorizes dispatch or ledger writes."""


class ExecutorUnavailable(RuntimeError):
    """Transport outcome is unknown; never evidence of execution failure."""


class Executor(Protocol):
    kind: str
    max_gpus: int

    async def lookup(self, submission_id: str) -> ExecutionStatus:
        """Observe an existing identity; NOT_FOUND alone is not terminal evidence."""
        ...

    async def submit(self, claim: Claim) -> ExecutionStatus:
        """Idempotent by submission_id, including across caller crashes.

        REJECTED means confirmed non-execution; FAILED means confirmed termination.
        Both must be definitive for this identity, including concurrent dispatches.
        A timeout or lost response is UNKNOWN, never REJECTED.
        """
        ...
