"""Configuration boundaries and parity of the explicit in-memory test adapter."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from kusanagi_nerv.db.connection import database_url
from kusanagi_nerv.domain.job_lifecycle import InvalidJobTransition, JobStatus
from kusanagi_nerv.domain.jobs import Job, JobResources, JobSpecification
from kusanagi_nerv.repositories.jobs import InMemoryJobRepository, JobUpdateConflict


@pytest.mark.parametrize(
    "value",
    [
        "not-a-database-url",
        "sqlite:///local.db",
        "postgresql://localhost/jobs",
        "postgresql+asyncpg://localhost",
        "postgresql+asyncpg://user:credential-marker@localhost:bad-port/jobs",
    ],
)
def test_invalid_database_configuration_does_not_echo_credentials(value: str) -> None:
    with pytest.raises(ValueError) as caught:
        database_url(value)
    assert "credential-marker" not in str(caught.value)
    assert "KUSANAGI_DATABASE_URL" in str(caught.value)


def test_database_url_is_read_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KUSANAGI_DATABASE_URL", "postgresql+asyncpg://localhost/jobs")
    assert database_url().database == "jobs"


async def test_memory_adapter_preserves_version_and_graph_checks() -> None:
    repository = InMemoryJobRepository()
    now = datetime.now(UTC)
    job = Job(
        job_id=uuid4(),
        tenant_id="tenant-a",
        spec=JobSpecification("tiny-mlp", "synthetic-v1", 1, JobResources(1, 0, 1), 0),
        status=JobStatus.QUEUED,
        created_at=now,
        updated_at=now,
    )
    await repository.add(job)
    running = await repository.update_status(
        tenant_id=job.tenant_id,
        job_id=job.job_id,
        expected_status=JobStatus.QUEUED,
        expected_version=0,
        new_status=JobStatus.RUNNING,
    )
    assert running.state_version == 1
    for tenant, expected, version in (
        (job.tenant_id, JobStatus.QUEUED, 0),
        ("tenant-b", JobStatus.RUNNING, 1),
    ):
        with pytest.raises(JobUpdateConflict):
            await repository.update_status(
                tenant_id=tenant,
                job_id=job.job_id,
                expected_status=expected,
                expected_version=version,
                new_status=JobStatus.FAILED,
            )
    with pytest.raises(InvalidJobTransition):
        await repository.update_status(
            tenant_id=job.tenant_id,
            job_id=job.job_id,
            expected_status=JobStatus.RUNNING,
            expected_version=1,
            new_status=JobStatus.CREATED,
        )
    assert await repository.get(tenant_id=job.tenant_id, job_id=job.job_id) == running
