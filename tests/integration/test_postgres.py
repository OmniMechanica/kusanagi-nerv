"""Migration, commit, rollback and competing-write behavior on real PostgreSQL."""

import asyncio
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from kusanagi_nerv.api.app import create_app
from kusanagi_nerv.db.connection import database_url
from kusanagi_nerv.db.models import JobRow
from kusanagi_nerv.domain.job_lifecycle import InvalidJobTransition, JobStatus
from kusanagi_nerv.domain.jobs import Job, JobResources, JobSpecification
from kusanagi_nerv.repositories.jobs import JobUpdateConflict, RepositoryUnavailable
from kusanagi_nerv.repositories.postgres import PostgresJobRepository

pytestmark = pytest.mark.integration


@pytest.fixture
def job() -> Job:
    now = datetime.now(UTC)
    return Job(
        job_id=uuid4(),
        tenant_id="tenant-a",
        spec=JobSpecification(
            "tiny-mlp", "synthetic-v1", 5, JobResources(0.5, 1, 2.5), 2
        ),
        status=JobStatus.QUEUED,
        created_at=now,
        updated_at=now,
    )


async def test_committed_job_round_trip(
    repository: PostgresJobRepository, job: Job, engine: AsyncEngine
) -> None:
    await repository.add(job)
    independent = PostgresJobRepository(
        async_sessionmaker(engine, expire_on_commit=False)
    )
    assert await independent.get(tenant_id=job.tenant_id, job_id=job.job_id) == job
    assert await repository.get(tenant_id="tenant-b", job_id=job.job_id) is None
    assert await repository.get(tenant_id=job.tenant_id, job_id=uuid4()) is None
    async with engine.connect() as connection:
        assert await connection.scalar(select(JobRow.status)) == "queued"
        assert await connection.scalar(select(JobRow.state_version)) == 0


async def test_all_job_metadata_survives_storage(
    repository: PostgresJobRepository, job: Job
) -> None:
    completed = replace(
        job,
        status=JobStatus.SUCCEEDED,
        state_version=3,
        attempt=1,
        checkpoint_uri="test://checkpoint",
        artifact_uri="test://artifact",
        result_model_id=uuid4(),
        result_model_version="v1",
        waiting_reason="observed",
        error="recorded diagnostic",
    )
    await repository.add(completed)
    assert (
        await repository.get(tenant_id=completed.tenant_id, job_id=completed.job_id)
        == completed
    )


async def test_payload_hash_is_canonical_but_does_not_deduplicate(
    repository: PostgresJobRepository,
    job: Job,
    engine: AsyncEngine,
) -> None:
    first = replace(job, spec=replace(job.spec, resources=JobResources(2, 0, 2)))
    second = replace(
        first,
        job_id=uuid4(),
        spec=replace(first.spec, resources=JobResources(2.0, 0, 2.0)),
    )
    third = replace(first, job_id=uuid4(), spec=replace(first.spec, epochs=6))
    for submitted in (first, second, third):
        await repository.add(submitted)
    async with engine.connect() as connection:
        rows = (
            await connection.execute(
                select(JobRow.job_id, JobRow.payload_hash, JobRow.idempotency_key)
            )
        ).all()
    digests = {row.id: row.payload_hash for row in rows}
    assert digests[first.job_id] == digests[second.job_id]
    assert digests[first.job_id] != digests[third.job_id]
    assert len(digests[first.job_id]) == 64
    assert all(row.idempotency_key is None for row in rows)


async def test_failed_write_rolls_back_and_next_operation_works(
    repository: PostgresJobRepository,
    job: Job,
    engine: AsyncEngine,
) -> None:
    await repository.add(job)
    with pytest.raises(RepositoryUnavailable):
        await repository.add(replace(job, tenant_id="attempted-overwrite"))
    assert await repository.get(tenant_id=job.tenant_id, job_id=job.job_id) == job
    await repository.add(replace(job, job_id=uuid4()))
    async with engine.connect() as connection:
        assert await connection.scalar(select(func.count()).select_from(JobRow)) == 2


async def test_two_writers_cannot_commit_the_same_version(
    repository: PostgresJobRepository,
    job: Job,
    engine: AsyncEngine,
) -> None:
    await repository.add(job)
    barrier = asyncio.Barrier(2)

    async def writer(target: JobStatus) -> Job | JobUpdateConflict:
        local = PostgresJobRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        )
        observed = await local.get(tenant_id=job.tenant_id, job_id=job.job_id)
        assert observed is not None
        await barrier.wait()
        try:
            return await local.update_status(
                tenant_id=job.tenant_id,
                job_id=job.job_id,
                expected_status=observed.status,
                expected_version=observed.state_version,
                new_status=target,
            )
        except JobUpdateConflict as exc:
            return exc

    outcomes = await asyncio.wait_for(
        asyncio.gather(writer(JobStatus.RUNNING), writer(JobStatus.CANCELLING)),
        timeout=15,
    )
    winners = [value for value in outcomes if isinstance(value, Job)]
    assert len(winners) == 1
    assert sum(isinstance(value, JobUpdateConflict) for value in outcomes) == 1
    stored = await repository.get(tenant_id=job.tenant_id, job_id=job.job_id)
    assert stored == winners[0]
    assert stored.state_version == 1


async def test_stale_version_is_rejected_even_after_state_cycles_back(
    repository: PostgresJobRepository,
    job: Job,
) -> None:
    await repository.add(job)
    current = job
    for status in (JobStatus.RUNNING, JobStatus.RETRYING, JobStatus.QUEUED):
        current = await repository.update_status(
            tenant_id=job.tenant_id,
            job_id=job.job_id,
            expected_status=current.status,
            expected_version=current.state_version,
            new_status=status,
        )
    with pytest.raises(JobUpdateConflict):
        await repository.update_status(
            tenant_id=job.tenant_id,
            job_id=job.job_id,
            expected_status=JobStatus.QUEUED,
            expected_version=0,
            new_status=JobStatus.RUNNING,
        )
    assert await repository.get(tenant_id=job.tenant_id, job_id=job.job_id) == current
    assert current.state_version == 3


@pytest.mark.parametrize("case", ["tenant", "id", "status"])
async def test_update_requires_matching_owner_identity_and_status(
    repository: PostgresJobRepository,
    job: Job,
    case: str,
) -> None:
    await repository.add(job)
    with pytest.raises(JobUpdateConflict):
        await repository.update_status(
            tenant_id="tenant-b" if case == "tenant" else job.tenant_id,
            job_id=uuid4() if case == "id" else job.job_id,
            expected_status=JobStatus.RUNNING if case == "status" else JobStatus.QUEUED,
            expected_version=0,
            new_status=JobStatus.FAILED,
        )
    assert await repository.get(tenant_id=job.tenant_id, job_id=job.job_id) == job


async def test_invalid_and_terminal_transitions_do_not_write(
    repository: PostgresJobRepository,
    job: Job,
) -> None:
    await repository.add(job)
    with pytest.raises(InvalidJobTransition):
        await repository.update_status(
            tenant_id=job.tenant_id,
            job_id=job.job_id,
            expected_status=JobStatus.QUEUED,
            expected_version=0,
            new_status=JobStatus.CREATED,
        )
    terminal = await repository.update_status(
        tenant_id=job.tenant_id,
        job_id=job.job_id,
        expected_status=JobStatus.QUEUED,
        expected_version=0,
        new_status=JobStatus.FAILED,
    )
    with pytest.raises(InvalidJobTransition):
        await repository.update_status(
            tenant_id=job.tenant_id,
            job_id=job.job_id,
            expected_status=JobStatus.FAILED,
            expected_version=1,
            new_status=JobStatus.QUEUED,
        )
    assert await repository.get(tenant_id=job.tenant_id, job_id=job.job_id) == terminal


@pytest.mark.parametrize(
    "values", [{"status": "invented"}, {"state_version": -1}, {"attempt": -1}]
)
async def test_database_rejects_invalid_ledger_values(
    repository: PostgresJobRepository,
    job: Job,
    engine: AsyncEngine,
    values: dict[str, object],
) -> None:
    await repository.add(job)
    with pytest.raises(IntegrityError):
        async with engine.begin() as connection:
            await connection.execute(
                update(JobRow).where(JobRow.job_id == job.job_id).values(**values)
            )
    assert await repository.get(tenant_id=job.tenant_id, job_id=job.job_id) == job


async def test_repeated_upgrade_preserves_data_and_schema_matches_models(
    repository: PostgresJobRepository,
    job: Job,
    engine: AsyncEngine,
    migrate: Callable[..., object],
) -> None:
    await repository.add(job)
    migrate("upgrade", "head")
    migrate("check")
    assert await repository.get(tenant_id=job.tenant_id, job_id=job.job_id) == job
    async with engine.connect() as connection:
        assert (
            await connection.scalar(text("SELECT version_num FROM alembic_version"))
            == "0001_job_ledger"
        )


def test_migration_downgrade_and_upgrade_on_owned_empty_database(
    migrate: Callable[..., object],
) -> None:
    migrate("upgrade", "head")
    migrate("downgrade", "base")
    migrate("upgrade", "head")
    migrate("check")


def test_unmigrated_database_returns_503_without_creating_schema(
    empty_database_url: str,
) -> None:
    with TestClient(create_app(database_url=empty_database_url)) as client:
        response = client.get(f"/v1/jobs/{uuid4()}")
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "storage_unavailable"

    async def check() -> None:
        import asyncpg

        url = database_url(empty_database_url).set(drivername="postgresql")
        connection = await asyncpg.connect(url.render_as_string(hide_password=False))
        try:
            assert (
                await connection.fetchval("SELECT to_regclass('public.jobs')") is None
            )
        finally:
            await connection.close()

    asyncio.run(check())


def test_driver_connection_failure_returns_controlled_503(
    empty_database_url: str,
) -> None:
    missing = database_url(empty_database_url).set(database=f"missing_{uuid4().hex}")
    with TestClient(
        create_app(database_url=missing.render_as_string(hide_password=False))
    ) as client:
        response = client.post(
            "/v1/jobs",
            json={
                "model": "tiny-mlp",
                "dataset": "synthetic-v1",
                "epochs": 5,
                "resources": {"cpus": 2, "memory_gib": 2},
            },
        )
    assert response.status_code == 503
    assert response.json() == {
        "error": {
            "code": "storage_unavailable",
            "message": "Job storage is unavailable",
            "details": [],
        }
    }
    assert "Location" not in response.headers
