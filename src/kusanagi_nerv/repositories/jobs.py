"""Replaceable job storage; the in-memory adapter is single-process only."""

from dataclasses import replace
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from kusanagi_nerv.domain.idempotency import canonical_spec, validate_key
from kusanagi_nerv.domain.job_lifecycle import JobStatus, transition
from kusanagi_nerv.domain.jobs import Job


class RepositoryUnavailable(RuntimeError):
    """A storage operation could not be completed or confirmed."""


class JobUpdateConflict(RuntimeError):
    """The expected tenant, status and version no longer identify an editable row."""


class IdempotencyConflict(RuntimeError):
    """This tenant/key already identifies a different canonical specification."""


class JobRepository(Protocol):
    async def admit(self, job: Job, *, idempotency_key: str | None = None) -> Job:
        """Commit a new admission or return the existing tenant/key job."""
        ...

    async def request_cancel(self, *, tenant_id: str, job_id: UUID) -> Job | None:
        """Persist cancellation intent; return None without revealing other tenants."""
        ...

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
        """Compare-and-swap before any attempt; dispatched jobs need lease guards."""
        ...


class InMemoryJobRepository:
    """Development adapter: records are lost on restart and not shared by workers."""

    def __init__(self) -> None:
        self._jobs: dict[UUID, Job] = {}
        self._keys: dict[tuple[str, str], tuple[str, UUID]] = {}

    async def admit(self, job: Job, *, idempotency_key: str | None = None) -> Job:
        validate_key(idempotency_key)
        if idempotency_key is None:
            await self.add(job)
            return job
        identity = (job.tenant_id, idempotency_key)
        digest = canonical_spec(job.spec)[1]
        if identity in self._keys:
            previous_digest, job_id = self._keys[identity]
            if previous_digest != digest:
                raise IdempotencyConflict("Idempotency key has a different payload")
            return self._jobs[job_id]
        await self.add(job)  # This test adapter does not suspend inside add().
        self._keys[identity] = (digest, job.job_id)
        return job

    async def request_cancel(self, *, tenant_id: str, job_id: UUID) -> Job | None:
        job = await self.get(tenant_id=tenant_id, job_id=job_id)
        if job is None or job.status in (
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.CANCELLING,
        ):
            return job
        safe = job.status == JobStatus.RETRYING or (
            job.status in (JobStatus.CREATED, JobStatus.QUEUED) and job.attempt == 0
        )
        now = datetime.now(UTC)
        updated = replace(
            job,
            status=transition(
                job.status, JobStatus.CANCELLED if safe else JobStatus.CANCELLING
            ),
            requested_cancel_at=now,
            retry_not_before=None,
            waiting_reason=None,
            state_version=job.state_version + 1,
            updated_at=now,
        )
        self._jobs[job_id] = updated
        return updated

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
            or job.attempt != 0
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
