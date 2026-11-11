"""Create the durable job ledger; execution attempts follow in MLP-004."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001_job_ledger"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("spec_json", postgresql.JSONB(), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column(
            "state_version", sa.Integer(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("attempt", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("checkpoint_uri", sa.Text(), nullable=True),
        sa.Column("artifact_uri", sa.Text(), nullable=True),
        sa.Column("result_model_id", sa.Uuid(), nullable=True),
        sa.Column("result_model_version", sa.String(128), nullable=True),
        sa.Column("waiting_reason", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "idempotency_key", name="uq_jobs_tenant_key"),
        sa.CheckConstraint(
            "status IN ('created', 'queued', 'running', 'retrying', 'cancelling', "
            "'succeeded', 'failed', 'cancelled')",
            name="ck_jobs_status",
        ),
        sa.CheckConstraint("state_version >= 0", name="ck_jobs_state_version"),
        sa.CheckConstraint("attempt >= 0", name="ck_jobs_attempt"),
        sa.CheckConstraint("length(tenant_id) > 0", name="ck_jobs_tenant"),
        sa.CheckConstraint(
            "jsonb_typeof(spec_json) = 'object'", name="ck_jobs_spec_object"
        ),
        sa.CheckConstraint(
            "payload_hash ~ '^[0-9a-f]{64}$'", name="ck_jobs_payload_hash"
        ),
    )
    op.create_index("ix_jobs_status_created_at", "jobs", ["status", "created_at"])
    op.create_index(
        "ix_jobs_tenant_created_at_id", "jobs", ["tenant_id", "created_at", "id"]
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_tenant_created_at_id", table_name="jobs")
    op.drop_index("ix_jobs_status_created_at", table_name="jobs")
    op.drop_table("jobs")
