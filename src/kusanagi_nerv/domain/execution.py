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
    CANCELLED = "cancelled"


class FailureKind(StrEnum):
    TRANSIENT = "transient"
    PERMANENT = "permanent"


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    worker_pid: int
    completed_steps: int
    checksum: str


@dataclass(frozen=True, slots=True)
class ExecutionObservation:
    status: ExecutionStatus
    # Only meaningful for a confirmed FAILED/REJECTED observation. Transport
    # failures are UNKNOWN and never enter the attempt retry policy.
    failure_kind: FailureKind = FailureKind.PERMANENT
    result: ExecutionResult | None = None


@dataclass(frozen=True, slots=True)
class Claim:
    job: Job
    attempt_id: UUID
    submission_id: str
    owner: str
    epoch: int
    accepted: bool
    executor_target: str = "local"


class LeaseLost(RuntimeError):
    """This claim no longer authorizes dispatch or ledger writes."""


class ExecutorUnavailable(RuntimeError):
    """Transport outcome is unknown; never evidence of execution failure."""


class Executor(Protocol):
    kind: str
    max_gpus: int
    target: str

    async def lookup(self, submission_id: str) -> ExecutionObservation:
        """Observe an existing identity; NOT_FOUND alone is not terminal evidence."""
        ...

    async def submit(self, claim: Claim) -> ExecutionObservation:
        """Idempotent by submission_id, including across caller crashes.

        REJECTED means confirmed non-execution; FAILED means confirmed termination.
        Both must be definitive for this identity, including concurrent dispatches.
        A timeout or lost response is UNKNOWN, never REJECTED.
        """
        ...

    async def cancel(self, submission_id: str) -> ExecutionObservation:
        """Request stop and observe the outcome for this identity.

        CANCELLED requires confirmed termination AND durable prevention of any
        delayed/concurrent submit for this identity. NOT_FOUND is not sufficient.
        Return UNKNOWN if the runtime cannot establish those guarantees.
        """
        ...
