"""Durable attempt identity, fenced leases and a local simulation runtime."""

import sqlalchemy as sa
from alembic import op

revision = "0002_attempt_leases"
down_revision = "0001_job_ledger"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_attempts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("submission_id", sa.String(128), nullable=False),
        sa.Column("executor_kind", sa.String(64), nullable=False),
        sa.Column("lease_owner", sa.String(128), nullable=False),
        sa.Column("lease_epoch", sa.Integer(), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("submission_intent_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observation", sa.String(16), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"]),
        sa.UniqueConstraint("job_id", "attempt_no", name="uq_attempts_job_number"),
        sa.UniqueConstraint("submission_id", name="uq_attempts_submission"),
        sa.CheckConstraint("attempt_no > 0", name="ck_attempts_number"),
        sa.CheckConstraint("lease_epoch > 0", name="ck_attempts_epoch"),
        sa.CheckConstraint("length(lease_owner) > 0", name="ck_attempts_owner"),
        sa.CheckConstraint(
            "last_observation IN ('not_found', 'unknown', 'pending', 'running', "
            "'succeeded', 'failed', 'rejected')",
            name="ck_attempts_observation",
        ),
        sa.CheckConstraint(
            "(finished_at IS NOT NULL) = "
            "(last_observation IN ('succeeded', 'failed', 'rejected'))",
            name="ck_attempts_terminal",
        ),
    )
    op.create_index(
        "uq_attempts_unfinished",
        "job_attempts",
        ["job_id"],
        unique=True,
        postgresql_where=sa.text("finished_at IS NULL"),
    )
    op.create_index(
        "ix_attempts_expiry",
        "job_attempts",
        ["executor_kind", "lease_expires_at"],
        postgresql_where=sa.text("finished_at IS NULL"),
    )
    op.create_table(
        "simulated_executions",
        sa.Column("submission_id", sa.String(128), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finish_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("submission_id"),
        sa.CheckConstraint("finish_at >= accepted_at", name="ck_simulation_duration"),
    )


def downgrade() -> None:
    op.drop_table("simulated_executions")
    op.drop_index("ix_attempts_expiry", table_name="job_attempts")
    op.drop_index("uq_attempts_unfinished", table_name="job_attempts")
    op.drop_table("job_attempts")
