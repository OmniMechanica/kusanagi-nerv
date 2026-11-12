"""Durable retry schedules, cancellation intent and simulated dispatch revocation."""

import sqlalchemy as sa
from alembic import op

revision = "0003_retry_cancel"
down_revision = "0002_attempt_leases"
branch_labels = None
depends_on = None


def observations(*, cancellation: bool) -> None:
    states = "'not_found', 'unknown', 'pending', 'running', 'succeeded', 'failed', 'rejected'"
    terminal = "'succeeded', 'failed', 'rejected'"
    if cancellation:
        states += ", 'cancelled'"
        terminal += ", 'cancelled'"
    op.create_check_constraint(
        "ck_attempts_observation", "job_attempts", f"last_observation IN ({states})"
    )
    op.create_check_constraint(
        "ck_attempts_terminal",
        "job_attempts",
        f"(finished_at IS NOT NULL) = (last_observation IN ({terminal}))",
    )


def upgrade() -> None:
    for name in ("requested_cancel_at", "retry_not_before"):
        op.add_column(
            "jobs", sa.Column(name, sa.DateTime(timezone=True), nullable=True)
        )
    op.create_index(
        "ix_jobs_retry_due",
        "jobs",
        ["retry_not_before"],
        postgresql_where=sa.text("status = 'retrying'"),
    )
    op.add_column(
        "job_attempts", sa.Column("failure_kind", sa.String(16), nullable=True)
    )
    op.create_check_constraint(
        "ck_attempts_failure_kind",
        "job_attempts",
        "failure_kind IN ('transient', 'permanent')",
    )
    op.drop_constraint("ck_attempts_observation", "job_attempts", type_="check")
    op.drop_constraint("ck_attempts_terminal", "job_attempts", type_="check")
    observations(cancellation=True)
    op.add_column(
        "simulated_executions",
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
    )
    for name in ("accepted_at", "finish_at"):
        op.alter_column("simulated_executions", name, nullable=True)
    op.create_check_constraint(
        "ck_simulation_identity",
        "simulated_executions",
        "(accepted_at IS NOT NULL AND finish_at IS NOT NULL) OR "
        "(accepted_at IS NULL AND finish_at IS NULL AND cancelled_at IS NOT NULL)",
    )


def downgrade() -> None:
    # New cancellation outcomes/tombstones cannot be represented in the old
    # schema. Constraints deliberately reject such a downgrade rather than erase them.
    op.drop_constraint("ck_simulation_identity", "simulated_executions", type_="check")
    for name in ("accepted_at", "finish_at"):
        op.alter_column("simulated_executions", name, nullable=False)
    op.drop_column("simulated_executions", "cancelled_at")
    op.drop_constraint("ck_attempts_observation", "job_attempts", type_="check")
    op.drop_constraint("ck_attempts_terminal", "job_attempts", type_="check")
    observations(cancellation=False)
    op.drop_constraint("ck_attempts_failure_kind", "job_attempts", type_="check")
    op.drop_column("job_attempts", "failure_kind")
    op.drop_index("ix_jobs_retry_due", table_name="jobs")
    op.drop_column("jobs", "retry_not_before")
    op.drop_column("jobs", "requested_cancel_at")
