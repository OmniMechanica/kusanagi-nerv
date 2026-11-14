"""Dispatch fencing uses actual PostgreSQL transactions, independently of Ray."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import uuid4

import httpx2
import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from kusanagi_nerv.controller.service import Controller
from kusanagi_nerv.db.models import AttemptRow, JobRow
from kusanagi_nerv.domain.execution import ExecutionResult, ExecutionStatus, LeaseLost
from kusanagi_nerv.domain.job_lifecycle import JobStatus
from kusanagi_nerv.domain.jobs import Job, JobResources, JobSpecification
from kusanagi_nerv.executors.ray_jobs import RayJobsExecutor, RayJobsSettings
from kusanagi_nerv.repositories.attempts import AttemptRepository
from kusanagi_nerv.repositories.postgres import PostgresJobRepository

pytestmark = pytest.mark.integration
TARGET = "http://127.0.0.1:8265#session_one"


@pytest.fixture
async def dispatch(engine, repository):
    now = datetime.now(UTC)
    job = Job(
        uuid4(),
        "test",
        JobSpecification("tiny-mlp", "synthetic-v1", 2, JobResources(1, 0, 1), 1),
        JobStatus.QUEUED,
        now,
        now,
    )
    await repository.add(job)
    store = AttemptRepository(async_sessionmaker(engine, expire_on_commit=False))
    claim = await store.claim_next(
        owner="first",
        executor_kind="ray-jobs-v1",
        executor_target=TARGET,
        lease_seconds=30,
    )
    assert claim is not None
    return store, claim


async def test_revocation_before_dispatch_blocks_delayed_submit(dispatch, repository):
    store, claim = dispatch
    await repository.request_cancel(
        tenant_id=claim.job.tenant_id, job_id=claim.job.job_id
    )
    record = await store.revoke_dispatch(claim.submission_id, TARGET)
    assert record.revoked and not record.started
    assert not await store.begin_dispatch(claim)
    record = await store.read_dispatch(claim.submission_id, TARGET)
    assert record.revoked and not record.started


async def test_revocation_preserves_possibly_in_flight_dispatch(
    dispatch, repository, engine
):
    store, claim = dispatch
    assert await store.begin_dispatch(claim)
    await repository.request_cancel(
        tenant_id=claim.job.tenant_id, job_id=claim.job.job_id
    )
    await store.revoke_dispatch(claim.submission_id, TARGET)
    restarted = AttemptRepository(async_sessionmaker(engine, expire_on_commit=False))
    record = await restarted.read_dispatch(claim.submission_id, TARGET)
    assert record.started and record.revoked
    assert not await restarted.begin_dispatch(claim)


async def test_cancel_adapter_requires_persisted_cancel_request(dispatch):
    store, claim = dispatch
    assert await store.revoke_dispatch(claim.submission_id, TARGET) is None
    assert await store.read_dispatch(claim.submission_id, TARGET + "-other") is None


async def test_recovery_is_pinned_to_recorded_cluster(dispatch, engine):
    store, claim = dispatch
    assert await store.begin_dispatch(claim)
    async with engine.begin() as connection:
        await connection.execute(
            update(AttemptRow).values(
                lease_expires_at=func.clock_timestamp() - text("interval '1 second'")
            )
        )
    assert (
        await store.claim_next(
            owner="wrong",
            executor_kind="ray-jobs-v1",
            executor_target=TARGET + "-other",
            lease_seconds=30,
        )
        is None
    )
    recovered = await store.claim_next(
        owner="replacement",
        executor_kind="ray-jobs-v1",
        executor_target=TARGET,
        lease_seconds=30,
    )
    assert recovered.attempt_id == claim.attempt_id
    assert recovered.executor_target == TARGET
    assert recovered.submission_id == claim.submission_id
    with pytest.raises(LeaseLost):
        await store.begin_dispatch(claim)


async def test_real_result_is_persisted_without_inventing_running_observation(
    dispatch, engine
):
    store, claim = dispatch
    result = ExecutionResult(123, 2, "a" * 64)
    assert await store.observe(claim, ExecutionStatus.SUCCEEDED, result=result) is None
    async with engine.connect() as connection:
        row = (await connection.execute(select(AttemptRow.__table__))).mappings().one()
        assert row["started_at"] is None
        assert row["execution_result"] == {
            "worker_pid": 123,
            "completed_steps": 2,
            "checksum": "a" * 64,
        }
        assert await connection.scalar(select(JobRow.status)) == "succeeded"


async def test_begin_dispatch_and_cancel_serialize_on_job_lock(dispatch, engine):
    store, claim = dispatch
    barrier = asyncio.Barrier(2)

    class RaceTransactions:
        @asynccontextmanager
        async def _transaction(self):
            async with super()._transaction() as session:
                await session.execute(select(1))
                await barrier.wait()
                yield session

    class DispatchRace(RaceTransactions, AttemptRepository):
        pass

    class CancelRace(RaceTransactions, PostgresJobRepository):
        pass

    sessions = async_sessionmaker(engine, expire_on_commit=False)
    admitted, _ = await asyncio.wait_for(
        asyncio.gather(
            DispatchRace(sessions).begin_dispatch(claim),
            CancelRace(sessions).request_cancel(
                tenant_id=claim.job.tenant_id, job_id=claim.job.job_id
            ),
        ),
        timeout=5,
    )
    record = await store.revoke_dispatch(claim.submission_id, TARGET)
    assert record.revoked
    assert record.started is admitted
    assert not await store.begin_dispatch(claim)


async def test_downgrade_does_not_remove_ray_dispatch_fences(dispatch, migrate, engine):
    with pytest.raises(
        AssertionError, match="Cannot downgrade while Ray attempt history exists"
    ):
        await asyncio.to_thread(migrate, "downgrade", "0003_retry_cancel")
    async with engine.connect() as connection:
        assert (
            await connection.scalar(text("SELECT version_num FROM alembic_version"))
            == "0004_ray_dispatch"
        )


async def test_unknown_ray_lookup_retains_attempt_without_dispatch_or_retry(
    dispatch, engine
):
    store, claim = dispatch
    requests = []

    def lost_response(request):
        requests.append(request.method)
        raise httpx2.ReadTimeout("Injected unreachable Ray", request=request)

    executor = RayJobsExecutor(
        store,
        RayJobsSettings("http://127.0.0.1:8265", "session_one", "a" * 64),
        transport=httpx2.MockTransport(lost_response),
    )
    controller = Controller(store, executor)
    controller.claim = claim
    try:
        for _ in range(3):
            await controller.tick()
        assert controller.claim.attempt_id == claim.attempt_id
        assert "POST" not in requests
        async with engine.connect() as connection:
            row = (
                (await connection.execute(select(AttemptRow.__table__)))
                .mappings()
                .one()
            )
            assert row["last_observation"] == "unknown"
            assert row["dispatch_started_at"] is None
            assert row["finished_at"] is None
            assert await connection.scalar(select(JobRow.attempt)) == 1
            assert (
                await connection.scalar(select(JobRow.waiting_reason))
                == "executor_outcome_unknown"
            )
    finally:
        await executor.aclose()
