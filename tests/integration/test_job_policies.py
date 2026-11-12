"""MLP-005 admission/retry/cancel races on PostgreSQL; execution is simulated."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from uuid import UUID, uuid4

import httpx2
import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from kusanagi_nerv.api.app import create_app
from kusanagi_nerv.controller.service import Controller
from kusanagi_nerv.db.models import AttemptRow, JobRow, SimulatedExecutionRow
from kusanagi_nerv.domain.execution import (
    Claim,
    ExecutionObservation,
    ExecutionStatus,
    FailureKind,
    LeaseLost,
)
from kusanagi_nerv.domain.job_lifecycle import JobStatus
from kusanagi_nerv.domain.retries import RetryPolicy
from kusanagi_nerv.executors.simulated import SimulatedExecutor
from kusanagi_nerv.repositories.attempts import AttemptRepository
from kusanagi_nerv.repositories.jobs import IdempotencyConflict, RepositoryUnavailable
from kusanagi_nerv.repositories.postgres import PostgresJobRepository
from kusanagi_nerv.repositories.transactions import TransactionalRepository

pytestmark = pytest.mark.integration
TENANT = "local-test-tenant"
PAYLOAD = {
    "model": "tiny-mlp",
    "dataset": "synthetic-v1",
    "epochs": 1,
    "resources": {"cpus": 1, "gpus": 0, "memory_gib": 1},
    "max_retries": 2,
}


@pytest.fixture
async def client(
    repository: PostgresJobRepository,
) -> AsyncIterator[httpx2.AsyncClient]:
    app = create_app(repository)
    async with (
        app.router.lifespan_context(app),
        httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app), base_url="http://test"
        ) as client,
    ):
        yield client


@pytest.fixture
def store(engine: AsyncEngine) -> AttemptRepository:
    return AttemptRepository(
        async_sessionmaker(engine, expire_on_commit=False),
        jitter=lambda: 0,
        retry_policy=RetryPolicy(base_seconds=60, cap_seconds=600),
    )


@pytest.fixture
def executor(engine: AsyncEngine) -> SimulatedExecutor:
    return SimulatedExecutor(
        async_sessionmaker(engine, expire_on_commit=False), duration_seconds=60
    )


async def submit(client: httpx2.AsyncClient) -> UUID:
    response = await client.post("/v1/jobs", json=PAYLOAD)
    assert response.status_code == 202
    return UUID(response.json()["job_id"])


async def claim_one(store: AttemptRepository, owner: str = "a") -> Claim:
    claim = await store.claim_next(
        owner=owner, executor_kind=SimulatedExecutor.kind, lease_seconds=30
    )
    assert claim is not None
    return claim


async def due_retry(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            update(JobRow)
            .where(JobRow.status == JobStatus.RETRYING)
            .values(retry_not_before=func.clock_timestamp() - timedelta(seconds=1))
        )


def transaction_barrier(
    monkeypatch: pytest.MonkeyPatch, *repos: TransactionalRepository
) -> None:
    """Synchronize one real transaction per supplied repository, then restore normal flow."""
    barrier = asyncio.Barrier(len(repos))

    def wrap(repository: TransactionalRepository):
        original = repository._transaction
        first = True

        @asynccontextmanager
        async def transaction() -> AsyncIterator[AsyncSession]:
            nonlocal first
            async with original() as session:
                if first:
                    first = False
                    await session.execute(select(1))
                    await barrier.wait()
                yield session

        return transaction

    for repo in repos:
        monkeypatch.setattr(repo, "_transaction", wrap(repo))


@pytest.mark.parametrize("different_payload", [False, True])
async def test_concurrent_http_admission_uses_one_durable_key(
    engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch, different_payload: bool
) -> None:
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    repos = [PostgresJobRepository(sessions), PostgresJobRepository(sessions)]
    transaction_barrier(monkeypatch, *repos)

    async def send(repo: PostgresJobRepository, epochs: int) -> httpx2.Response:
        app = create_app(repo)
        async with (
            app.router.lifespan_context(app),
            httpx2.AsyncClient(
                transport=httpx2.ASGITransport(app), base_url="http://test"
            ) as client,
        ):
            return await client.post(
                "/v1/jobs",
                json={**PAYLOAD, "epochs": epochs},
                headers={"Idempotency-Key": "concurrent"},
            )

    responses = await asyncio.wait_for(
        asyncio.gather(
            send(repos[0], 1), send(repos[1], 2 if different_payload else 1)
        ),
        timeout=10,
    )
    assert sorted(r.status_code for r in responses) == (
        [202, 409] if different_payload else [202, 202]
    )
    if not different_payload:
        assert responses[0].json() == responses[1].json()
        assert responses[0].headers["Location"] == responses[1].headers["Location"]
    async with engine.connect() as connection:
        assert await connection.scalar(select(func.count()).select_from(JobRow)) == 1
        assert (
            await connection.scalar(
                text(
                    "SELECT contype::text FROM pg_constraint WHERE conname = 'uq_jobs_tenant_key'"
                )
            )
            == "u"
        )


async def test_key_replay_canonicalization_tenant_scope_and_uncertain_commit(
    client: httpx2.AsyncClient,
    repository: PostgresJobRepository,
    engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = repository.admit

    async def lose_reply(job, *, idempotency_key=None):
        await original(job, idempotency_key=idempotency_key)
        raise RepositoryUnavailable("response lost after commit")

    monkeypatch.setattr(repository, "admit", lose_reply)
    minimal = {
        "model": "m",
        "dataset": "d",
        "epochs": 1,
        "resources": {"cpus": 1, "memory_gib": 1},
    }
    headers = {"Idempotency-Key": "recover"}
    assert (
        await client.post("/v1/jobs", json=minimal, headers=headers)
    ).status_code == 503
    monkeypatch.setattr(repository, "admit", original)
    explicit = {
        **minimal,
        "max_retries": 0,
        "resources": {"cpus": 1.0, "memory_gib": 1.0, "gpus": 0},
    }
    replay = await client.post("/v1/jobs", json=explicit, headers=headers)
    assert replay.status_code == 202
    job = await repository.get(tenant_id=TENANT, job_id=UUID(replay.json()["job_id"]))
    assert job is not None
    other = await original(
        replace(job, job_id=uuid4(), tenant_id="other"), idempotency_key="recover"
    )
    assert other.job_id != job.job_id
    assert await repository.request_cancel(tenant_id="other", job_id=job.job_id) is None
    with pytest.raises(IdempotencyConflict):
        await original(
            replace(job, spec=replace(job.spec, max_retries=1)),
            idempotency_key="recover",
        )
    async with engine.connect() as connection:
        assert await connection.scalar(select(func.count()).select_from(JobRow)) == 2


@pytest.mark.parametrize("max_retries", [0, 2])
async def test_retry_budget_timestamps_and_competing_retry_claims(
    client: httpx2.AsyncClient,
    repository: PostgresJobRepository,
    store: AttemptRepository,
    engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
    max_retries: int,
) -> None:
    response = await client.post(
        "/v1/jobs", json={**PAYLOAD, "max_retries": max_retries}
    )
    assert response.status_code == 202
    job_id = UUID(response.json()["job_id"])
    claim = await claim_one(store)
    identities = {claim.submission_id}
    for attempt_no in range(1, max_retries + 2):
        assert claim.job.attempt == attempt_no and claim.job.job_id == job_id
        assert (
            await store.observe(
                claim, ExecutionStatus.FAILED, failure_kind=FailureKind.TRANSIENT
            )
            is None
        )
        job = await repository.get(tenant_id=TENANT, job_id=job_id)
        assert job is not None
        with pytest.raises(LeaseLost):
            await store.observe(
                claim, ExecutionStatus.FAILED, failure_kind=FailureKind.TRANSIENT
            )
        if attempt_no == max_retries + 1:
            assert job.status == JobStatus.FAILED and job.retry_not_before is None
            break
        assert job.status == JobStatus.RETRYING and job.retry_not_before is not None
        async with engine.connect() as connection:
            finished = await connection.scalar(
                select(AttemptRow.finished_at).where(
                    AttemptRow.attempt_no == attempt_no
                )
            )
        assert (job.retry_not_before - finished).total_seconds() == 30 * 2 ** (
            attempt_no - 1
        )
        replacement = AttemptRepository(
            async_sessionmaker(engine, expire_on_commit=False), jitter=lambda: 1
        )
        assert (
            await replacement.claim_next(
                owner="b", executor_kind=SimulatedExecutor.kind, lease_seconds=30
            )
            is None
        )
        assert (
            await repository.get(tenant_id=TENANT, job_id=job_id)
        ).retry_not_before == job.retry_not_before
        await due_retry(engine)
        transaction_barrier(monkeypatch, store, replacement)
        candidates = await asyncio.wait_for(
            asyncio.gather(
                *(
                    candidate.claim_next(
                        owner=owner,
                        executor_kind=SimulatedExecutor.kind,
                        lease_seconds=30,
                    )
                    for candidate, owner in ((store, "a"), (replacement, "b"))
                )
            ),
            timeout=5,
        )
        winners = [c for c in candidates if c is not None]
        assert len(winners) == 1
        claim = winners[0]
        assert claim.submission_id not in identities
        identities.add(claim.submission_id)
    assert (
        await store.claim_next(
            owner="a", executor_kind=SimulatedExecutor.kind, lease_seconds=30
        )
        is None
    )
    async with engine.connect() as connection:
        assert (
            await connection.scalar(select(func.count()).select_from(AttemptRow))
            == max_retries + 1
        )


async def test_controller_retries_confirmed_transient_failure_then_succeeds(
    client: httpx2.AsyncClient,
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = await submit(client)
    attempts = []

    async def execute(claim: Claim) -> ExecutionObservation:
        attempts.append(claim)
        return (
            ExecutionObservation(ExecutionStatus.FAILED, FailureKind.TRANSIENT)
            if claim.job.attempt == 1
            else ExecutionObservation(ExecutionStatus.SUCCEEDED)
        )

    monkeypatch.setattr(executor, "submit", execute)
    await Controller(store, executor).tick()
    assert (await client.get(f"/v1/jobs/{job_id}")).json()["status"] == "retrying"
    await due_retry(engine)
    await Controller(store, executor).tick()
    result = (await client.get(f"/v1/jobs/{job_id}")).json()
    assert result["status"] == "succeeded" and result["attempt"] == 2
    assert attempts[0].submission_id != attempts[1].submission_id
    assert result["retry_not_before"] is None and result["error"] is None


async def test_cancellation_during_backoff_suppresses_retry(
    client: httpx2.AsyncClient, store: AttemptRepository, engine: AsyncEngine
) -> None:
    job_id = await submit(client)
    claim = await claim_one(store)
    await store.observe(
        claim, ExecutionStatus.REJECTED, failure_kind=FailureKind.TRANSIENT
    )
    response = await client.post(f"/v1/jobs/{job_id}/cancel")
    assert response.status_code == 200 and response.json()["status"] == "cancelled"
    assert response.json()["retry_not_before"] is None
    await due_retry(engine)
    assert (
        await store.claim_next(
            owner="b", executor_kind=SimulatedExecutor.kind, lease_seconds=30
        )
        is None
    )
    async with engine.connect() as connection:
        assert (
            await connection.scalar(select(func.count()).select_from(AttemptRow)) == 1
        )


@pytest.mark.parametrize("retrying", [False, True])
async def test_cancel_and_claim_are_serialized(
    client: httpx2.AsyncClient,
    repository: PostgresJobRepository,
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
    retrying: bool,
) -> None:
    job_id = await submit(client)
    if retrying:
        previous = await claim_one(store)
        await store.observe(
            previous, ExecutionStatus.FAILED, failure_kind=FailureKind.TRANSIENT
        )
        await due_retry(engine)
    transaction_barrier(monkeypatch, repository, store)
    cancelled, claim = await asyncio.wait_for(
        asyncio.gather(
            client.post(f"/v1/jobs/{job_id}/cancel"),
            store.claim_next(owner="a", executor_kind=executor.kind, lease_seconds=30),
        ),
        timeout=5,
    )
    if claim is None:
        assert cancelled.status_code == 200 and cancelled.json()["attempt"] == int(
            retrying
        )
    else:
        assert (
            cancelled.status_code == 202 and cancelled.json()["status"] == "cancelling"
        )
        controller = Controller(store, executor, owner="a")
        controller.claim = claim
        await controller.tick()
    assert (await client.get(f"/v1/jobs/{job_id}")).json()["status"] == "cancelled"
    async with engine.connect() as connection:
        # A fence record may exist, but no work was accepted by the simulator.
        assert (
            await connection.scalar(
                select(func.count())
                .select_from(SimulatedExecutionRow)
                .where(SimulatedExecutionRow.accepted_at.is_not(None))
            )
            == 0
        )


@pytest.mark.parametrize("outcome", [ExecutionStatus.SUCCEEDED, ExecutionStatus.FAILED])
async def test_cancel_racing_terminal_observation_keeps_observed_outcome(
    client: httpx2.AsyncClient,
    repository: PostgresJobRepository,
    store: AttemptRepository,
    monkeypatch: pytest.MonkeyPatch,
    outcome: ExecutionStatus,
) -> None:
    job_id = await submit(client)
    claim = await claim_one(store)
    transaction_barrier(monkeypatch, repository, store)
    await asyncio.wait_for(
        asyncio.gather(
            client.post(f"/v1/jobs/{job_id}/cancel"), store.observe(claim, outcome)
        ),
        timeout=5,
    )
    current = (await client.get(f"/v1/jobs/{job_id}")).json()
    assert current["status"] == outcome.value
    repeated = await client.post(f"/v1/jobs/{job_id}/cancel")
    assert repeated.status_code == 200 and repeated.json() == current


@pytest.mark.parametrize("lost_reply", [False, True])
async def test_cancel_running_or_unknown_accepted_submit_requires_confirmation(
    client: httpx2.AsyncClient,
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
    lost_reply: bool,
) -> None:
    job_id = await submit(client)
    original_submit, original_cancel = executor.submit, executor.cancel

    async def dispatch(claim: Claim) -> ExecutionObservation:
        result = await original_submit(claim)
        if lost_reply:
            raise TimeoutError("accepted, reply lost")
        return result

    async def uncertain_cancel(submission_id: str) -> ExecutionObservation:
        return ExecutionObservation(ExecutionStatus.NOT_FOUND)

    monkeypatch.setattr(executor, "submit", dispatch)
    controller = Controller(store, executor)
    await controller.tick()
    response = await client.post(f"/v1/jobs/{job_id}/cancel")
    assert response.status_code == 202 and response.json()["status"] == "cancelling"
    assert (await client.post(f"/v1/jobs/{job_id}/cancel")).json() == response.json()
    monkeypatch.setattr(executor, "cancel", uncertain_cancel)
    for _ in range(3):
        await controller.tick()
    pending = (await client.get(f"/v1/jobs/{job_id}")).json()
    assert pending["status"] == "cancelling" and pending["attempt"] == 1
    monkeypatch.setattr(executor, "cancel", original_cancel)
    await controller.tick()
    final = (await client.get(f"/v1/jobs/{job_id}")).json()
    assert final["status"] == "cancelled" and final["attempt"] == 1
    async with engine.connect() as connection:
        assert (
            await connection.scalar(select(func.count()).select_from(AttemptRow)) == 1
        )
        assert (
            await connection.scalar(select(SimulatedExecutionRow.cancelled_at))
            is not None
        )


async def test_late_dispatch_after_cancel_confirmation_is_revoked(
    client: httpx2.AsyncClient,
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = await submit(client)
    original_submit = executor.submit
    entered, release = asyncio.Event(), asyncio.Event()
    observed = []

    async def delayed_submit(claim: Claim) -> ExecutionObservation:
        entered.set()
        await release.wait()
        result = await original_submit(claim)
        observed.append(result.status)
        return result

    monkeypatch.setattr(executor, "submit", delayed_submit)
    old = Controller(store, executor, owner="old")
    task = asyncio.create_task(old.tick())
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        assert (await client.post(f"/v1/jobs/{job_id}/cancel")).status_code == 202
        async with engine.begin() as connection:
            await connection.execute(
                update(AttemptRow).values(
                    lease_expires_at=func.clock_timestamp() - timedelta(seconds=1)
                )
            )
        await Controller(store, executor, owner="replacement").tick()
        final = (await client.get(f"/v1/jobs/{job_id}")).json()
        assert final["status"] == "cancelled"
        release.set()
        await asyncio.wait_for(task, timeout=5)
        assert observed == [ExecutionStatus.CANCELLED] and old.claim is None
        assert (await client.get(f"/v1/jobs/{job_id}")).json() == final
        async with engine.connect() as connection:
            row = (
                await connection.execute(select(SimulatedExecutionRow.__table__))
            ).one()
            assert row.accepted_at is None and row.cancelled_at is not None
            assert await connection.scalar(select(AttemptRow.lease_epoch)) == 2
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_cancellation_suppresses_retry_even_for_transient_failure(
    client: httpx2.AsyncClient, store: AttemptRepository
) -> None:
    job_id = await submit(client)
    claim = await claim_one(store)
    assert (await client.post(f"/v1/jobs/{job_id}/cancel")).status_code == 202
    await store.observe(
        claim, ExecutionStatus.FAILED, failure_kind=FailureKind.TRANSIENT
    )
    result = (await client.get(f"/v1/jobs/{job_id}")).json()
    assert result["status"] == "failed" and result["retry_not_before"] is None
    assert (
        await store.claim_next(
            owner="b", executor_kind=SimulatedExecutor.kind, lease_seconds=30
        )
        is None
    )


async def test_simulator_preserves_completion_when_cancel_arrives_late(
    client: httpx2.AsyncClient, store: AttemptRepository, engine: AsyncEngine
) -> None:
    job_id = await submit(client)
    claim = await claim_one(store)
    instant = SimulatedExecutor(
        async_sessionmaker(engine, expire_on_commit=False), duration_seconds=0
    )
    assert (await instant.submit(claim)).status == ExecutionStatus.SUCCEEDED
    assert (await client.post(f"/v1/jobs/{job_id}/cancel")).status_code == 202
    controller = Controller(store, instant, owner=claim.owner)
    controller.claim = claim
    await controller.tick()
    assert (await client.get(f"/v1/jobs/{job_id}")).json()["status"] == "succeeded"


async def test_simultaneous_runtime_submit_and_cancel_leave_a_durable_fence(
    client: httpx2.AsyncClient,
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await submit(client)
    claim = await claim_one(store)
    other = SimulatedExecutor(
        async_sessionmaker(engine, expire_on_commit=False), duration_seconds=60
    )
    transaction_barrier(monkeypatch, executor, other)
    await asyncio.wait_for(
        asyncio.gather(executor.submit(claim), other.cancel(claim.submission_id)),
        timeout=5,
    )
    assert (
        await executor.lookup(claim.submission_id)
    ).status == ExecutionStatus.CANCELLED
    assert (await executor.submit(claim)).status == ExecutionStatus.CANCELLED
    async with engine.connect() as connection:
        assert (
            await connection.scalar(
                select(func.count()).select_from(SimulatedExecutionRow)
            )
            == 1
        )


async def test_runtime_lookup_waits_for_cancellation_transaction(
    client: httpx2.AsyncClient,
    store: AttemptRepository,
    executor: SimulatedExecutor,
    engine: AsyncEngine,
) -> None:
    await submit(client)
    claim = await claim_one(store)
    await executor.submit(claim)
    lookup = None
    try:
        async with engine.begin() as cancelling:
            await cancelling.execute(
                update(SimulatedExecutionRow).values(
                    cancelled_at=func.clock_timestamp()
                )
            )
            lookup = asyncio.create_task(executor.lookup(claim.submission_id))
            async with asyncio.timeout(3), engine.connect() as probe:
                while not await probe.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE datname = current_database() AND wait_event_type = 'Lock'"
                    )
                ):
                    assert not lookup.done(), "Lookup bypassed the cancellation lock"
                    await asyncio.sleep(0.01)
        assert (await lookup).status == ExecutionStatus.CANCELLED
    finally:
        if lookup is not None and not lookup.done():
            lookup.cancel()
            await asyncio.gather(lookup, return_exceptions=True)
