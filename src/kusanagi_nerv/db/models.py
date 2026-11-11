"""Schema metadata for the durable logical-job ledger."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class JobRow(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('created', 'queued', 'running', 'retrying', 'cancelling', "
            "'succeeded', 'failed', 'cancelled')",
            name="ck_jobs_status",
        ),
        CheckConstraint("state_version >= 0", name="ck_jobs_state_version"),
        CheckConstraint("attempt >= 0", name="ck_jobs_attempt"),
        CheckConstraint("length(tenant_id) > 0", name="ck_jobs_tenant"),
        CheckConstraint(
            "jsonb_typeof(spec_json) = 'object'", name="ck_jobs_spec_object"
        ),
        CheckConstraint("payload_hash ~ '^[0-9a-f]{64}$'", name="ck_jobs_payload_hash"),
        UniqueConstraint("tenant_id", "idempotency_key", name="uq_jobs_tenant_key"),
        Index("ix_jobs_status_created_at", "status", "created_at"),
        Index("ix_jobs_tenant_created_at_id", "tenant_id", "created_at", "id"),
    )

    job_id: Mapped[UUID] = mapped_column("id", primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128))
    spec_json: Mapped[dict[str, Any]] = mapped_column(JSONB)
    payload_hash: Mapped[str] = mapped_column(String(64))
    # Reserved for MLP-005. The current API leaves this NULL and does not deduplicate.
    idempotency_key: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(16))
    state_version: Mapped[int] = mapped_column(server_default=text("0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    attempt: Mapped[int] = mapped_column(server_default=text("0"))
    checkpoint_uri: Mapped[str | None] = mapped_column(Text)
    artifact_uri: Mapped[str | None] = mapped_column(Text)
    result_model_id: Mapped[UUID | None]
    result_model_version: Mapped[str | None] = mapped_column(String(128))
    waiting_reason: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)


class AttemptRow(Base):
    __tablename__ = "job_attempts"
    __table_args__ = (
        UniqueConstraint("job_id", "attempt_no", name="uq_attempts_job_number"),
        UniqueConstraint("submission_id", name="uq_attempts_submission"),
        CheckConstraint("attempt_no > 0", name="ck_attempts_number"),
        CheckConstraint("lease_epoch > 0", name="ck_attempts_epoch"),
        CheckConstraint("length(lease_owner) > 0", name="ck_attempts_owner"),
        CheckConstraint(
            "last_observation IN ('not_found', 'unknown', 'pending', 'running', "
            "'succeeded', 'failed', 'rejected')",
            name="ck_attempts_observation",
        ),
        CheckConstraint(
            "(finished_at IS NOT NULL) = "
            "(last_observation IN ('succeeded', 'failed', 'rejected'))",
            name="ck_attempts_terminal",
        ),
        Index(
            "uq_attempts_unfinished",
            "job_id",
            unique=True,
            postgresql_where=text("finished_at IS NULL"),
        ),
        Index(
            "ix_attempts_expiry",
            "executor_kind",
            "lease_expires_at",
            postgresql_where=text("finished_at IS NULL"),
        ),
    )

    attempt_id: Mapped[UUID] = mapped_column("id", primary_key=True)
    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id"))
    attempt_no: Mapped[int]
    submission_id: Mapped[str] = mapped_column(String(128))
    executor_kind: Mapped[str] = mapped_column(String(64))
    lease_owner: Mapped[str] = mapped_column(String(128))
    lease_epoch: Mapped[int]
    lease_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    submission_intent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_observation: Mapped[str] = mapped_column(String(16))
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class SimulatedExecutionRow(Base):
    """Durable test runtime, deliberately separate from controller authority."""

    __tablename__ = "simulated_executions"
    __table_args__ = (
        CheckConstraint("finish_at >= accepted_at", name="ck_simulation_duration"),
    )

    submission_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    accepted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finish_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
