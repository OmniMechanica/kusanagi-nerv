"""Persist an attempt's published and selected resume checkpoint references."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0005_training_checkpoints"
down_revision = "0004_ray_dispatch"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name in ("published_checkpoint", "resume_checkpoint"):
        op.add_column("job_attempts", sa.Column(name, JSONB(), nullable=True))


def downgrade() -> None:
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM job_attempts WHERE executor_kind = 'ray-training-v1')"
        )
    ):
        raise RuntimeError("Cannot downgrade while training attempt history exists")
    for name in ("resume_checkpoint", "published_checkpoint"):
        op.drop_column("job_attempts", name)
