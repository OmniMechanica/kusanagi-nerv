"""Require job model results to match their source job, tenant and version."""

import sqlalchemy as sa
from alembic import op

revision = "0009_job_model_result"
down_revision = "0008_model_deployments"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Repair reserved legacy metadata using the registered model as authority.
    # Jobs without a matching model keep their status and artifact evidence.
    op.execute(
        sa.text("""
        WITH links AS (
            SELECT j.id AS job_id, m.id AS model_id, m.version
            FROM jobs j
            LEFT JOIN models m
              ON j.status = 'succeeded'
             AND m.source_job_id = j.id AND m.tenant_id = j.tenant_id
        )
        UPDATE jobs j
        SET result_model_id = links.model_id, result_model_version = links.version,
            state_version = j.state_version + 1, updated_at = clock_timestamp()
        FROM links
        WHERE j.id = links.job_id
          AND (j.result_model_id IS DISTINCT FROM links.model_id
               OR j.result_model_version IS DISTINCT FROM links.version)
    """)
    )
    op.create_unique_constraint(
        "uq_models_result_link",
        "models",
        ["id", "source_job_id", "tenant_id", "version"],
    )
    op.create_check_constraint(
        "ck_jobs_model_result",
        "jobs",
        "(result_model_id IS NULL AND result_model_version IS NULL) OR "
        "(status = 'succeeded' AND result_model_id IS NOT NULL "
        "AND result_model_version IS NOT NULL)",
    )
    op.create_foreign_key(
        "fk_jobs_model_result",
        "jobs",
        "models",
        ["result_model_id", "id", "tenant_id", "result_model_version"],
        ["id", "source_job_id", "tenant_id", "version"],
        deferrable=True,
        initially="DEFERRED",
    )


def downgrade() -> None:
    op.drop_constraint("fk_jobs_model_result", "jobs", type_="foreignkey")
    op.drop_constraint("ck_jobs_model_result", "jobs", type_="check")
    op.drop_constraint("uq_models_result_link", "models", type_="unique")
