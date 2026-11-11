"""Schema metadata for the durable logical-job ledger."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
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
