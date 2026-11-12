"""Real PostgreSQL authority/recovery; executor outcomes here are SIMULATED."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from kusanagi_nerv.controller.service import Controller, ControllerSettings
from kusanagi_nerv.db.models import AttemptRow, JobRow, SimulatedExecutionRow
from kusanagi_nerv.domain.execution import (
    Claim,
    ExecutionObservation,
    ExecutionStatus,
    LeaseLost,
)
from kusanagi_nerv.domain.job_lifecycle import JobStatus
from kusanagi_nerv.domain.jobs import Job, JobResources, JobSpecification
from kusanagi_nerv.executors.simulated import SimulatedExecutor
from kusanagi_nerv.repositories.attempts import AttemptRepository
from kusanagi_nerv.repositories.jobs import JobUpdateConflict, RepositoryUnavailable
from kusanagi_nerv.repositories.postgres import PostgresJobRepository

pytestmark = pytest.mark.integration


@pytest.fixture
async def job(repository: PostgresJobRepository) -> Job:
    now = datetime.now(UTC)
    job = Job(
        job_id=uuid4(),
        tenant_id="tenant-a",
        spec=JobSpecification("tiny-mlp", "synthetic-v1", 1, JobResources(1, 0, 1), 2),
        status=JobStatus.QUEUED,
        created_at=now,
        updated_at=now,
    )
    await repository.add(job)
    return job


@pytest.fixture
def store(engine: AsyncEngine) -> AttemptRepository:
    return AttemptRepository(async_sessionmaker(engine, expire_on_commit=False))


@pytest.fixture
def executor(engine: AsyncEngine) -> SimulatedExecutor:
    return SimulatedExecutor(
        async_sessionmaker(engine, expire_on_commit=False), duration_seconds=0
    )


async def claim_one(store: AttemptRepository, owner: str = "controller-a") -> Claim:
    claim = await store.claim_next(
        owner=owner, executor_kind=SimulatedExecutor.kind, lease_seconds=30
    )
    assert claim is not None
    return claim


async def expire(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            update(AttemptRow).values(
                lease_expires_at=func.clock_timestamp() - timedelta(seconds=1)
            )
        )


async def test_two_controller_transactions_claim_exactly_one_attempt(
    engine: AsyncEngine, job: Job
) -> None:
    barrier = asyncio.Barrier(2)

    class BarrierStore(AttemptRepository):
        @asynccontextmanager
        async def _transaction(self) -> AsyncIterator[AsyncSession]:
            async with super()._transaction() as session:
                # Both independent connections enter a real DB transaction before
                # either can select/lock the job. No sleep is used to stage the race.
                await session.execute(select(1))
                await barrier.wait()
                yield session

    sessions = async_sessionmaker(engine, expire_on_commit=False)
    stores = [BarrierStore(sessions), BarrierStore(sessions)]
    claims = await asyncio.wait_for(
        asyncio.gather(
            *(
                store.claim_next(
                    owner=f"controller-{i}",
                    executor_kind=SimulatedExecutor.kind,
                    lease_seconds=30,
                )
                for i, store in enumerate(stores)
            )
        ),
        timeout=5,
    )
    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1
    assert winners[0].job.job_id == job.job_id
    async with engine.connect() as connection:
        assert (
            await connection.scalar(select(func.count()).select_from(AttemptRow)) == 1
        )
        assert await connection.scalar(select(JobRow.attempt)) == 1


async def test_locked_job_is_skipped_and_other_job_can_be_claimed(
    store: AttemptRepository,
    engine: AsyncEngine,
    repository: PostgresJobRepository,
    job: Job,
) -> None:
    other = replace(job, job_id=uuid4(), tenant_id="tenant-b")
    await repository.add(other)
    async with engine.begin() as locked:
        await locked.execute(
            select(JobRow).where(JobRow.job_id == job.job_id).with_for_update()
        )
        claim = await asyncio.wait_for(claim_one(store), timeout=2)
        assert claim.job.job_id == other.job_id
        assert (
            await asyncio.wait_for(
                store.claim_next(
                    owner="controller-b",
                    executor_kind=SimulatedExecutor.kind,
                    lease_seconds=30,
                ),
                timeout=2,
            )
            is None
        )


async def test_expiry_reuses_identity_and_fences_old_owner(
    store: AttemptRepository, engine: AsyncEngine, job: Job
) -> None:
    old = await claim_one(store)
    await expire(engine)
    current = await claim_one(store, "controller-b")
    assert current.attempt_id == old.attempt_id
    assert current.submission_id == old.submission_id
    assert current.epoch == old.epoch + 1
    assert current.job.attempt == 1
    for stale in (old, replace(current, owner=old.owner), replace(current, epoch=1)):
        with pytest.raises(LeaseLost):
            await store.observe(stale, ExecutionStatus.SUCCEEDED)
        with pytest.raises(LeaseLost):
            await store.renew(stale, lease_seconds=30)
    await store.observe(current, ExecutionStatus.SUCCEEDED)
    with pytest.raises(LeaseLost):
        await store.observe(current, ExecutionStatus.FAILED)


async def test_expired_owner_cannot_renew_or_finish_before_takeover(
    store: AttemptRepository, engine: AsyncEngine, job: Job
) -> None:
    claim = await claim_one(store)
    await expire(engine)
    with pytest.raises(LeaseLost):
        await store.renew(claim, lease_seconds=30)
    with pytest.raises(LeaseLost):
        await store.observe(claim, ExecutionStatus.SUCCEEDED)


async def test_live_heartbeat_blocks_takeover_and_wrong_executor_cannot_recover(
    store: AttemptRepository, engine: AsyncEngine, job: Job
) -> None:
    claim = await claim_one(store)
    assert await store.renew(claim, lease_seconds=60) == claim
    assert (
        await store.claim_next(
            owner="b", executor_kind=SimulatedExecutor.kind, lease_seconds=30
        )
        is None
    )
    await expire(engine)
    assert (
        await store.claim_next(
            owner="b", executor_kind="different-runtime", lease_seconds=30
        )
        is None
    )


async def test_unfenced_status_update_cannot_cancel_dispatched_job(
    store: AttemptRepository, repository: PostgresJobRepository, job: Job
) -> None:
    claim = await claim_one(store)
    with pytest.raises(JobUpdateConflict):
        await repository.update_status(
            tenant_id=job.tenant_id,
            job_id=job.job_id,
            expected_status=JobStatus.QUEUED,
            expected_version=claim.job.state_version,
            new_status=JobStatus.CANCELLED,
        )


async def test_submission_intent_is_committed_before_dispatch_and_fast_success(
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    repository: PostgresJobRepository,
    job: Job,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = executor.submit

    async def submit(claim: Claim) -> ExecutionObservation:
        async with engine.connect() as connection:
            record = (await connection.execute(select(AttemptRow.__table__))).one()
            assert record.id == claim.attempt_id
            assert record.submission_id == claim.submission_id
            assert record.submission_intent_at is not None
            assert record.finished_at is None
        return await original(claim)

    monkeypatch.setattr(executor, "submit", submit)
    await Controller(store, executor).tick()
    result = await repository.get(tenant_id=job.tenant_id, job_id=job.job_id)
    assert result is not None and result.status == JobStatus.SUCCEEDED
    assert result.attempt == 1 and result.result_model_id is None
    async with engine.connect() as connection:
        # Fast terminal observation does not invent a RUNNING timestamp.
        assert await connection.scalar(select(AttemptRow.started_at)) is None


async def test_lost_submit_reply_recovers_same_execution_without_new_attempt(
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    repository: PostgresJobRepository,
    job: Job,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = executor.submit
    submissions = []

    async def lose_reply(claim: Claim) -> ExecutionObservation:
        submissions.append(claim.submission_id)
        await original(claim)
        raise TimeoutError("reply lost after acceptance")

    monkeypatch.setattr(executor, "submit", lose_reply)
    first = Controller(store, executor)
    await first.tick()
    queued = await repository.get(tenant_id=job.tenant_id, job_id=job.job_id)
    assert queued is not None and queued.status == JobStatus.QUEUED
    assert queued.waiting_reason == "executor_outcome_unknown"
    await expire(engine)
    await Controller(store, executor).tick()
    result = await repository.get(tenant_id=job.tenant_id, job_id=job.job_id)
    assert result is not None and result.status == JobStatus.SUCCEEDED
    assert result.attempt == 1 and len(submissions) == 1
    async with engine.connect() as connection:
        assert await connection.scalar(select(AttemptRow.lease_epoch)) == 2
        assert (
            await connection.scalar(
                select(func.count()).select_from(SimulatedExecutionRow)
            )
            == 1
        )


@pytest.mark.parametrize("outcome", [ExecutionStatus.REJECTED, ExecutionStatus.FAILED])
async def test_confirmed_failure_finalizes_once_without_retry_policy(
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    repository: PostgresJobRepository,
    job: Job,
    monkeypatch: pytest.MonkeyPatch,
    outcome: ExecutionStatus,
) -> None:
    async def submit(claim: Claim) -> ExecutionObservation:
        return ExecutionObservation(outcome)

    monkeypatch.setattr(executor, "submit", submit)
    controller = Controller(store, executor)
    await controller.tick()
    await controller.tick()
    result = await repository.get(tenant_id=job.tenant_id, job_id=job.job_id)
    assert result is not None and result.status == JobStatus.FAILED
    assert result.attempt == 1 and result.error == f"executor_{outcome.value}"
    async with engine.connect() as connection:
        assert await connection.scalar(select(AttemptRow.finished_at)) is not None
        accepted = await connection.scalar(select(AttemptRow.accepted_at))
        assert (accepted is not None) == (outcome == ExecutionStatus.FAILED)


async def test_unknown_lookup_does_not_dispatch_and_call_is_bounded(
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    job: Job,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def lookup(submission_id: str) -> ExecutionObservation:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setattr(executor, "lookup", lookup)
    controller = Controller(
        store,
        executor,
        settings=ControllerSettings(lease_seconds=30, executor_timeout_seconds=0.02),
    )
    await asyncio.wait_for(controller.tick(), timeout=2)
    assert controller.claim is not None
    async with engine.connect() as connection:
        assert await connection.scalar(select(AttemptRow.last_observation)) == "unknown"
        assert (
            await connection.scalar(
                select(func.count()).select_from(SimulatedExecutionRow)
            )
            == 0
        )


async def test_accepted_execution_disappearing_is_not_resubmitted_or_failed(
    store: AttemptRepository,
    executor: SimulatedExecutor,
    job: Job,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claim = await claim_one(store)
    accepted = await store.observe(claim, ExecutionStatus.RUNNING)
    assert accepted is not None and accepted.accepted
    controller = Controller(store, executor, owner=claim.owner)
    controller.claim = accepted

    async def forbidden_submit(claim: Claim) -> ExecutionObservation:
        raise AssertionError("must reconcile missing accepted execution")

    monkeypatch.setattr(executor, "submit", forbidden_submit)
    await controller.tick()  # simulator has no row, so lookup returns NOT_FOUND
    assert controller.claim is not None
    assert controller.claim.job.status == JobStatus.RUNNING
    assert controller.claim.job.waiting_reason == "executor_outcome_unknown"


async def test_lease_lost_during_external_call_cannot_commit_success(
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    repository: PostgresJobRepository,
    job: Job,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replacement = None

    async def delayed_success(claim: Claim) -> ExecutionObservation:
        nonlocal replacement
        await expire(engine)
        replacement = await claim_one(store, "controller-b")
        return ExecutionObservation(ExecutionStatus.SUCCEEDED)

    monkeypatch.setattr(executor, "submit", delayed_success)
    controller = Controller(store, executor)
    await controller.tick()
    assert controller.claim is None and replacement is not None
    result = await repository.get(tenant_id=job.tenant_id, job_id=job.job_id)
    assert result is not None and result.status == JobStatus.QUEUED
    await store.observe(replacement, ExecutionStatus.SUCCEEDED)


async def test_storage_failure_before_claim_cannot_dispatch(
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    job: Job,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failed_claim(**kwargs: object) -> Claim | None:
        raise RepositoryUnavailable("unconfirmed")

    monkeypatch.setattr(store, "claim_next", failed_claim)
    await Controller(store, executor).tick()
    async with engine.connect() as connection:
        assert (
            await connection.scalar(select(func.count()).select_from(AttemptRow)) == 0
        )
        assert (
            await connection.scalar(
                select(func.count()).select_from(SimulatedExecutionRow)
            )
            == 0
        )


async def test_gpu_job_stays_queued_without_attempt_or_cpu_fallback(
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    repository: PostgresJobRepository,
) -> None:
    now = datetime.now(UTC)
    gpu = Job(
        job_id=uuid4(),
        tenant_id="gpu-tenant",
        spec=JobSpecification("m", "d", 1, JobResources(1, 1, 1), 0),
        status=JobStatus.QUEUED,
        created_at=now,
        updated_at=now,
    )
    await repository.add(gpu)
    await Controller(store, executor).tick()
    assert await repository.get(tenant_id=gpu.tenant_id, job_id=gpu.job_id) == gpu
    async with engine.connect() as connection:
        assert (
            await connection.scalar(select(func.count()).select_from(AttemptRow)) == 0
        )


async def test_simulator_duplicate_dispatch_is_idempotent(
    store: AttemptRepository, executor: SimulatedExecutor, engine: AsyncEngine, job: Job
) -> None:
    claim = await claim_one(store)
    assert await asyncio.gather(executor.submit(claim), executor.submit(claim)) == [
        ExecutionObservation(ExecutionStatus.SUCCEEDED),
        ExecutionObservation(ExecutionStatus.SUCCEEDED),
    ]
    async with engine.connect() as connection:
        assert (
            await connection.scalar(
                select(func.count()).select_from(SimulatedExecutionRow)
            )
            == 1
        )


async def test_database_rejects_second_unfinished_attempt(
    store: AttemptRepository, engine: AsyncEngine, job: Job
) -> None:
    await claim_one(store)
    async with engine.connect() as connection:
        record = (
            (await connection.execute(select(AttemptRow.__table__))).mappings().one()
        )
    duplicate = dict(record, id=uuid4(), attempt_no=2, submission_id=str(uuid4()))
    with pytest.raises(IntegrityError):
        async with engine.begin() as connection:
            await connection.execute(AttemptRow.__table__.insert().values(**duplicate))


async def test_crash_before_dispatch_recovers_persisted_intent(
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    job: Job,
) -> None:
    original = await claim_one(store)
    await expire(engine)
    await Controller(store, executor).tick()
    async with engine.connect() as connection:
        assert (
            await connection.scalar(select(AttemptRow.attempt_id))
            == original.attempt_id
        )
        assert (
            await connection.scalar(select(SimulatedExecutionRow.submission_id))
            == original.submission_id
        )
        assert await connection.scalar(select(JobRow.status)) == "succeeded"


async def test_expiry_is_checked_after_waiting_for_job_lock(
    store: AttemptRepository,
    engine: AsyncEngine,
    job: Job,
) -> None:
    claim = await claim_one(store)
    writer = None
    try:
        async with engine.begin() as locked:
            await locked.execute(select(JobRow).with_for_update())
            # The writer begins with a valid lease, then blocks on our row lock.
            writer = asyncio.create_task(
                store.observe(claim, ExecutionStatus.SUCCEEDED)
            )
            async with asyncio.timeout(3):
                async with engine.connect() as probe:
                    while not await probe.scalar(
                        text(
                            "SELECT count(*) FROM pg_stat_activity "
                            "WHERE datname = current_database() AND wait_event_type = 'Lock'"
                        )
                    ):
                        await asyncio.sleep(0.01)
            # Expire before releasing the lock. The guarded write must use DB
            # wall-clock time after waiting, not its earlier transaction timestamp.
            await locked.execute(
                update(AttemptRow).values(lease_expires_at=func.clock_timestamp())
            )
        with pytest.raises(LeaseLost):
            await writer
    finally:
        if writer is not None and not writer.done():
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)


async def test_transport_retry_before_acceptance_reuses_submission_id(
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    job: Job,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = executor.submit
    identities = []

    async def submit(claim: Claim) -> ExecutionObservation:
        identities.append(claim.submission_id)
        if len(identities) == 1:
            raise TimeoutError("request never reached executor")
        return await original(claim)

    monkeypatch.setattr(executor, "submit", submit)
    controller = Controller(store, executor)
    await controller.tick()
    await controller.tick()
    assert len(identities) == 2 and identities[0] == identities[1]
    async with engine.connect() as connection:
        assert await connection.scalar(select(JobRow.attempt)) == 1
        assert await connection.scalar(select(JobRow.status)) == "succeeded"
