"""Replaceable job storage; the in-memory adapter is single-process only."""

from dataclasses import replace
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from kusanagi_nerv.domain.job_lifecycle import JobStatus, transition
from kusanagi_nerv.domain.jobs import Job


class RepositoryUnavailable(RuntimeError):
    """A storage operation could not be completed or confirmed."""


class JobUpdateConflict(RuntimeError):
    """The expected tenant, status and version no longer identify an editable row."""


class JobRepository(Protocol):
    async def add(self, job: Job) -> None:
        """Store an admitted job before returning acceptance to the caller."""
        ...

    async def get(self, *, tenant_id: str, job_id: UUID) -> Job | None:
        """Return a job owned by this tenant, or None without revealing ownership."""
        ...

    async def update_status(
        self,
        *,
        tenant_id: str,
        job_id: UUID,
        expected_status: JobStatus,
        expected_version: int,
        new_status: JobStatus,
    ) -> Job:
        """Compare-and-swap the graph state; callers must also enforce execution guards."""
        ...


class InMemoryJobRepository:
    """Development adapter: records are lost on restart and not shared by workers."""

    def __init__(self) -> None:
        self._jobs: dict[UUID, Job] = {}

    async def add(self, job: Job) -> None:
        if job.job_id in self._jobs:
            raise ValueError("Job ID already exists")
        self._jobs[job.job_id] = job

    async def get(self, *, tenant_id: str, job_id: UUID) -> Job | None:
        job = self._jobs.get(job_id)
        if job is None or job.tenant_id != tenant_id:
            return None
        return job

    async def update_status(
        self,
        *,
        tenant_id: str,
        job_id: UUID,
        expected_status: JobStatus,
        expected_version: int,
        new_status: JobStatus,
    ) -> Job:
        status = transition(expected_status, new_status)
        job = self._jobs.get(job_id)
        if (
            job is None
            or job.tenant_id != tenant_id
            or job.status != expected_status
            or job.state_version != expected_version
        ):
            raise JobUpdateConflict("Job state changed or job is unavailable")
        updated = replace(
            job,
            status=status,
            state_version=job.state_version + 1,
            updated_at=datetime.now(UTC),
        )
        self._jobs[job_id] = updated
        return updated
