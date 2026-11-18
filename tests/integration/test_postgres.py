"""Migration, commit, rollback and competing-write behavior on real PostgreSQL."""

import asyncio
from collections.abc import Callable
from dataclasses import asdict, replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from kusanagi_nerv.api.app import create_app
from kusanagi_nerv.db.connection import database_url
from kusanagi_nerv.db.models import AttemptRow, JobRow, ModelRow
from kusanagi_nerv.domain.execution import (
    CheckpointRef,
    ExecutionResult,
    ExecutionStatus,
    FailureKind,
    LeaseLost,
    TrainingExecutionResult,
)
from kusanagi_nerv.domain.job_lifecycle import InvalidJobTransition, JobStatus
from kusanagi_nerv.domain.jobs import Job, JobResources, JobSpecification
from kusanagi_nerv.repositories.attempts import AttemptRepository
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
    # In this owned DB the new tables are empty. Keep the existing job while
    # returning to the MLP-003 schema, then verify the forward upgrade twice.
    migrate("downgrade", "0001_job_ledger")
    migrate("upgrade", "head")
    migrate("upgrade", "head")
    migrate("check")
    assert await repository.get(tenant_id=job.tenant_id, job_id=job.job_id) == job
    async with engine.connect() as connection:
        assert (
            await connection.scalar(text("SELECT version_num FROM alembic_version"))
            == "0009_job_model_result"
        )


def test_migration_downgrade_and_upgrade_on_owned_empty_database(
    migrate: Callable[..., object],
) -> None:
    migrate("upgrade", "head")
    migrate("downgrade", "base")
    migrate("upgrade", "head")
    migrate("check")


async def test_upgrade_preserves_a_pending_training_retry(
    repository: PostgresJobRepository,
    job: Job,
    engine: AsyncEngine,
    migrate: Callable[..., object],
) -> None:
    job = replace(job, spec=replace(job.spec, resources=JobResources(2, 0, 0.25)))
    await repository.add(job)
    attempts = AttemptRepository(async_sessionmaker(engine, expire_on_commit=False))
    claim_options = {
        "owner": "training-controller",
        "executor_kind": "ray-training-v1",
        "executor_target": "test-training-target",
        "lease_seconds": 30,
    }
    first = await attempts.claim_next(**claim_options)
    assert first is not None
    checkpoint = CheckpointRef(
        f"s3://test-bucket/jobs/{job.job_id}/attempts/{first.submission_id}/checkpoints/0002.pt",
        "a" * 64,
        2,
    )
    await attempts.observe(
        first,
        ExecutionStatus.FAILED,
        failure_kind=FailureKind.TRANSIENT,
        checkpoint=checkpoint,
        retry_checkpoint=checkpoint,
    )
    migrate("downgrade", "0005_training_checkpoints")
    migrate("upgrade", "head")
    async with engine.begin() as connection:
        await connection.execute(
            update(JobRow)
            .where(JobRow.job_id == job.job_id, JobRow.status == JobStatus.RETRYING)
            .values(retry_not_before=func.clock_timestamp())
        )
    successor = await attempts.claim_next(**claim_options)
    assert successor is not None
    assert successor.job.attempt == 2 and successor.resume_checkpoint == checkpoint


async def test_training_success_registers_one_model_under_competing_observations(
    repository: PostgresJobRepository, job: Job, engine: AsyncEngine
) -> None:
    job = replace(job, spec=replace(job.spec, resources=JobResources(2, 0, 0.25)))
    await repository.add(job)
    attempts = AttemptRepository(async_sessionmaker(engine, expire_on_commit=False))
    claim = await attempts.claim_next(
        owner="training-controller",
        executor_kind="ray-training-v1",
        lease_seconds=30,
    )
    assert claim is not None
    result = TrainingExecutionResult(
        worker_pid=123,
        completed_steps=job.spec.epochs,
        checksum="a" * 64,
        artifact_uri=f"s3://test-bucket/jobs/{job.job_id}/attempts/{claim.submission_id}/model.pt",
        first_epoch=1,
        restored_optimizer_step=0,
    )
    for incomplete in (None, ExecutionResult(123, 5, "a" * 64)):
        observed = await attempts.observe(
            claim, ExecutionStatus.SUCCEEDED, result=incomplete
        )
        assert observed is not None
        assert observed.attempt_id == claim.attempt_id
        assert observed.job.status is JobStatus.QUEUED
        assert observed.job.waiting_reason == "executor_outcome_unknown"
        assert observed.job.result_model_id is None
        claim = observed
    with pytest.raises(RepositoryUnavailable):
        await attempts.observe(
            claim, ExecutionStatus.SUCCEEDED, result=replace(result, checksum="bad")
        )
    pending = await repository.get(tenant_id=job.tenant_id, job_id=job.job_id)
    assert pending is not None and pending.status == JobStatus.QUEUED
    assert pending.result_model_id is None and pending.artifact_uri is None
    async with engine.connect() as connection:
        assert await connection.scalar(select(ModelRow.model_id)) is None
        assert await connection.scalar(select(AttemptRow.finished_at)) is None

    outcomes = await asyncio.gather(
        attempts.observe(claim, ExecutionStatus.SUCCEEDED, result=result),
        attempts.observe(claim, ExecutionStatus.SUCCEEDED, result=result),
        return_exceptions=True,
    )
    assert outcomes.count(None) == 1
    assert sum(isinstance(outcome, LeaseLost) for outcome in outcomes) == 1
    completed = await repository.get(tenant_id=job.tenant_id, job_id=job.job_id)
    assert completed is not None and completed.status == JobStatus.SUCCEEDED
    assert (
        completed.result_model_id is not None and completed.result_model_version == "v1"
    )
    assert completed.artifact_uri == result.artifact_uri
    async with engine.connect() as connection:
        model = (await connection.execute(select(ModelRow.__table__))).mappings().one()
        assert model["id"] == completed.result_model_id
        assert (
            model["source_job_id"] == job.job_id and model["tenant_id"] == job.tenant_id
        )
        assert model["version"] == completed.result_model_version
        assert model["artifact_uri"] == result.artifact_uri
        assert model["artifact_sha256"] == result.checksum


async def test_upgrade_registers_preexisting_verified_training_success(
    repository: PostgresJobRepository,
    job: Job,
    engine: AsyncEngine,
    migrate: Callable[..., object],
) -> None:
    await asyncio.to_thread(migrate, "downgrade", "0006_retry_checkpoint")
    job = replace(job, spec=replace(job.spec, resources=JobResources(2, 0, 0.25)))
    await repository.add(job)
    attempts = AttemptRepository(async_sessionmaker(engine, expire_on_commit=False))
    claim = await attempts.claim_next(
        owner="training-controller",
        executor_kind="ray-training-v1",
        lease_seconds=30,
    )
    assert claim is not None
    result = TrainingExecutionResult(
        worker_pid=123,
        completed_steps=job.spec.epochs,
        checksum="a" * 64,
        artifact_uri=f"s3://test-bucket/jobs/{job.job_id}/attempts/{claim.submission_id}/model.pt",
        first_epoch=1,
        restored_optimizer_step=0,
    )
    # Write the legacy terminal observation using the pre-registry schema.
    async with engine.begin() as connection:
        await connection.execute(
            update(JobRow)
            .where(JobRow.job_id == job.job_id)
            .values(status=JobStatus.SUCCEEDED, artifact_uri=result.artifact_uri)
        )
        await connection.execute(
            update(AttemptRow)
            .where(AttemptRow.attempt_id == claim.attempt_id)
            .values(
                execution_result=asdict(result),
                last_observation=ExecutionStatus.SUCCEEDED,
                finished_at=func.clock_timestamp(),
            )
        )
    unverified = replace(
        job,
        job_id=uuid4(),
        status=JobStatus.SUCCEEDED,
        artifact_uri=result.artifact_uri,
    )
    await repository.add(unverified)
    await asyncio.to_thread(migrate, "upgrade", "head")
    completed = await repository.get(tenant_id=job.tenant_id, job_id=job.job_id)
    assert completed is not None and completed.result_model_id is not None
    assert completed.result_model_version == "v1"
    assert (
        await repository.get(tenant_id=job.tenant_id, job_id=unverified.job_id)
        == unverified
    )
    async with engine.connect() as connection:
        model = (await connection.execute(select(ModelRow.__table__))).mappings().one()
        assert model["id"] == completed.result_model_id
        assert (
            model["source_job_id"] == job.job_id and model["tenant_id"] == job.tenant_id
        )
        assert model["artifact_uri"] == result.artifact_uri
        assert model["artifact_sha256"] == result.checksum
    await asyncio.to_thread(migrate, "upgrade", "head")
    assert await repository.get(tenant_id=job.tenant_id, job_id=job.job_id) == completed


async def test_upgrade_repairs_and_enforces_job_model_links(
    repository: PostgresJobRepository,
    job: Job,
    engine: AsyncEngine,
    migrate: Callable[..., object],
) -> None:
    await asyncio.to_thread(migrate, "downgrade", "0008_model_deployments")
    completed = replace(
        job, status=JobStatus.SUCCEEDED, artifact_uri="s3://test/model.pt"
    )
    model_id = uuid4()
    legacy = replace(completed, job_id=uuid4(), result_model_id=model_id)
    await repository.add(completed)
    await repository.add(legacy)
    async with engine.begin() as connection:
        await connection.execute(
            ModelRow.__table__.insert().values(
                id=model_id,
                source_job_id=completed.job_id,
                tenant_id=completed.tenant_id,
                version="v1",
                artifact_uri=completed.artifact_uri,
                artifact_sha256="a" * 64,
            )
        )
        await connection.execute(
            update(JobRow)
            .where(JobRow.job_id == completed.job_id)
            .values(result_model_id=uuid4(), result_model_version="v2")
        )

    await asyncio.to_thread(migrate, "upgrade", "head")
    registered = await repository.get(tenant_id=job.tenant_id, job_id=completed.job_id)
    assert registered is not None
    assert (
        registered.result_model_id == model_id
        and registered.result_model_version == "v1"
    )
    repaired = await repository.get(tenant_id=job.tenant_id, job_id=legacy.job_id)
    assert repaired is not None
    assert repaired.result_model_id is None and repaired.result_model_version is None
    assert (
        repaired.status is JobStatus.SUCCEEDED
        and repaired.artifact_uri == legacy.artifact_uri
    )

    for job_id, invalid_id, invalid_version in (
        (completed.job_id, uuid4(), "v1"),
        (legacy.job_id, model_id, "v1"),
        (completed.job_id, model_id, "v2"),
        (completed.job_id, model_id, None),
    ):
        with pytest.raises(IntegrityError):
            async with engine.begin() as connection:
                await connection.execute(
                    update(JobRow)
                    .where(JobRow.job_id == job_id)
                    .values(
                        result_model_id=invalid_id, result_model_version=invalid_version
                    )
                )
    assert (
        await repository.get(tenant_id=job.tenant_id, job_id=completed.job_id)
        == registered
    )


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
