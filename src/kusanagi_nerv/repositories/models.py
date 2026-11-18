"""Tenant-scoped model reads and guarded deployment updates."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from kusanagi_nerv.db.models import JobRow, ModelRow
from kusanagi_nerv.domain.job_lifecycle import JobStatus
from kusanagi_nerv.domain.models import DeployState, Model
from kusanagi_nerv.repositories.transactions import TransactionalRepository

# Serializes publishers of the ledger's complete Serve application config.
SERVE_RECONCILE_LOCK = 0x4B4E5345525645


def to_model(row: ModelRow) -> Model:
    return Model(
        row.model_id,
        row.version,
        row.artifact_uri,
        row.artifact_sha256,
        DeployState(row.deploy_state),
        row.deploy_revision,
        row.deploy_error,
    )


def owned_model(tenant_id: str, model_id: UUID):
    return (
        select(ModelRow)
        .join(JobRow, ModelRow.source_job_id == JobRow.job_id)
        .where(
            ModelRow.model_id == model_id,
            ModelRow.tenant_id == tenant_id,
            JobRow.tenant_id == tenant_id,
            JobRow.status == JobStatus.SUCCEEDED,
        )
    )


class ModelRepository(TransactionalRepository):
    async def get(self, *, tenant_id: str, model_id: UUID) -> Model | None:
        async with self._transaction() as session:
            row = await session.scalar(owned_model(tenant_id, model_id))
            return to_model(row) if row is not None else None

    async def request_deploy(self, *, tenant_id: str, model_id: UUID) -> Model | None:
        async with self._transaction() as session:
            row = await session.scalar(
                owned_model(tenant_id, model_id).with_for_update(of=ModelRow)
            )
            if row is None:
                return None
            if row.deploy_state in (DeployState.NOT_DEPLOYED, DeployState.FAILED):
                row.deploy_state = DeployState.DEPLOYING
                row.deploy_revision += 1
                row.deploy_error = None
                await session.flush()
            return to_model(row)

    @asynccontextmanager
    async def reconcile(self) -> AsyncIterator[AsyncSession | None]:
        async with self._transaction() as session:
            acquired = await session.scalar(
                select(func.pg_try_advisory_xact_lock(SERVE_RECONCILE_LOCK))
            )
            yield session if acquired else None

    async def requested_models(self, session: AsyncSession) -> list[Model]:
        rows = await session.scalars(
            select(ModelRow)
            .where(ModelRow.deploy_revision > 0)
            .order_by(ModelRow.model_id)
        )
        return [to_model(row) for row in rows]

    async def observe(
        self, session: AsyncSession, model: Model, state: DeployState
    ) -> None:
        if state != model.deploy_state:
            await session.execute(
                update(ModelRow)
                .where(
                    ModelRow.model_id == model.model_id,
                    ModelRow.deploy_revision == model.deploy_revision,
                )
                .values(
                    deploy_state=state,
                    deploy_error="serve_deployment_failed"
                    if state is DeployState.FAILED
                    else None,
                )
            )
