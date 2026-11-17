"""Persist the verified checkpoint chosen at terminal training failure."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0006_retry_checkpoint"
down_revision = "0005_training_checkpoints"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("job_attempts", sa.Column("retry_checkpoint", JSONB(), nullable=True))
    # Under revision 0005 transient training failures could only resume from
    # their own published checkpoint. Preserve that choice for pending retries.
    op.execute(
        sa.text("""
        UPDATE job_attempts SET retry_checkpoint = published_checkpoint
        WHERE executor_kind = 'ray-training-v1' AND failure_kind = 'transient'
    """)
    )


def downgrade() -> None:
    if op.get_bind().scalar(
        sa.text("""
        SELECT EXISTS (SELECT 1 FROM job_attempts
        WHERE retry_checkpoint IS NOT NULL AND retry_checkpoint <> 'null'::jsonb
        AND retry_checkpoint IS DISTINCT FROM published_checkpoint)
    """)
    ):
        raise RuntimeError("Cannot downgrade while inherited retry choices exist")
    op.drop_column("job_attempts", "retry_checkpoint")
