"""Persist desired deployment and the last confirmed Serve state."""

import sqlalchemy as sa
from alembic import op

revision = "0008_model_deployments"
down_revision = "0007_model_registry"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "models",
        sa.Column(
            "deploy_state", sa.String(16), nullable=False, server_default="not_deployed"
        ),
    )
    op.add_column(
        "models",
        sa.Column(
            "deploy_revision", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
    )
    op.add_column("models", sa.Column("deploy_error", sa.String(128)))
    op.create_check_constraint(
        "ck_models_deploy_state",
        "models",
        "(deploy_state = 'not_deployed' AND deploy_revision = 0) OR "
        "(deploy_state IN ('deploying', 'ready', 'failed') AND deploy_revision > 0)",
    )


def downgrade() -> None:
    if op.get_bind().scalar(
        sa.text("SELECT EXISTS (SELECT 1 FROM models WHERE deploy_revision > 0)")
    ):
        raise RuntimeError("Cannot downgrade while deployment history exists")
    op.drop_constraint("ck_models_deploy_state", "models", type_="check")
    for name in ("deploy_error", "deploy_revision", "deploy_state"):
        op.drop_column("models", name)
