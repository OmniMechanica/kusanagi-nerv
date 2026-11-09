"""Replaceable job storage; the in-memory adapter is single-process only."""

from typing import Protocol
from uuid import UUID

from kusanagi_nerv.domain.jobs import Job


class JobRepository(Protocol):
    async def add(self, job: Job) -> None:
        """Store an admitted job before returning acceptance to the caller."""
        ...

    async def get(self, *, tenant_id: str, job_id: UUID) -> Job | None:
        """Return a job owned by this tenant, or None without revealing ownership."""
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
