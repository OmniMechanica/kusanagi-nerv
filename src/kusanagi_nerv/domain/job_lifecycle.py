"""Pure job-state transitions, independent of persistence and execution.

This graph expresses which transitions are possible. The service/controller must
also check execution evidence and ownership before persisting a transition.
"""

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Final


class JobStatus(StrEnum):
    """Logical job states with stable lowercase values for serialization."""

    CREATED = "created"
    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    CANCELLING = "cancelling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


_ALLOWED_TRANSITIONS: Final[Mapping[JobStatus, frozenset[JobStatus]]] = (
    MappingProxyType(
        {
            JobStatus.CREATED: frozenset({JobStatus.QUEUED, JobStatus.CANCELLED}),
            JobStatus.QUEUED: frozenset(
                {
                    JobStatus.RUNNING,
                    JobStatus.SUCCEEDED,
                    JobStatus.RETRYING,
                    JobStatus.FAILED,
                    JobStatus.CANCELLING,
                    JobStatus.CANCELLED,
                }
            ),
            JobStatus.RUNNING: frozenset(
                {
                    JobStatus.SUCCEEDED,
                    JobStatus.RETRYING,
                    JobStatus.FAILED,
                    JobStatus.CANCELLING,
                }
            ),
            JobStatus.RETRYING: frozenset(
                {JobStatus.QUEUED, JobStatus.FAILED, JobStatus.CANCELLED}
            ),
            JobStatus.CANCELLING: frozenset(
                {JobStatus.CANCELLED, JobStatus.FAILED, JobStatus.SUCCEEDED}
            ),
            JobStatus.SUCCEEDED: frozenset(),
            JobStatus.FAILED: frozenset(),
            JobStatus.CANCELLED: frozenset(),
        }
    )
)


def can_transition(current: JobStatus, new: JobStatus) -> bool:
    """Check the graph only; both arguments must already be JobStatus values."""
    if not isinstance(current, JobStatus) or not isinstance(new, JobStatus):
        raise TypeError("current and new must be JobStatus values")
    return new in _ALLOWED_TRANSITIONS[current]


class InvalidJobTransition(ValueError):
    """A requested edge is absent from the job lifecycle graph."""

    def __init__(self, current: JobStatus, new: JobStatus) -> None:
        self.current = current
        self.new = new
        super().__init__(
            f"Cannot transition job from '{current.value}' to '{new.value}'."
        )


def transition(current: JobStatus, new: JobStatus) -> JobStatus:
    """Return the requested state or raise InvalidJobTransition; mutate nothing."""
    if not can_transition(current, new):
        raise InvalidJobTransition(current, new)
    return new
