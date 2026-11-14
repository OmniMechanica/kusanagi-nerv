"""Pin executor targets and persist dispatch admission/revocation and results."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0004_ray_dispatch"
down_revision = "0003_retry_cancel"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "job_attempts",
        sa.Column(
            "executor_target", sa.String(512), nullable=False, server_default="local"
        ),
    )
    for name in ("dispatch_started_at", "dispatch_revoked_at"):
        op.add_column(
            "job_attempts", sa.Column(name, sa.DateTime(timezone=True), nullable=True)
        )
    op.add_column(
        "job_attempts", sa.Column("execution_result", postgresql.JSONB(), nullable=True)
    )


def downgrade() -> None:
    # Do not discard active Ray dispatch fences or reinterpret a Ray attempt as
    # a simulated/local attempt. An operator must retain/export this history.
    connection = op.get_bind()
    if connection.scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM job_attempts WHERE executor_kind = 'ray-jobs-v1')"
        )
    ):
        raise RuntimeError("Cannot downgrade while Ray attempt history exists")
    for name in (
        "execution_result",
        "dispatch_revoked_at",
        "dispatch_started_at",
        "executor_target",
    ):
        op.drop_column("job_attempts", name)
