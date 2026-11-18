"""Register one immutable model per successful training job."""

import sqlalchemy as sa
from alembic import op

revision = "0007_model_registry"
down_revision = "0006_retry_checkpoint"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "models",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("source_job_id", sa.Uuid(), sa.ForeignKey("jobs.id"), nullable=False),
        sa.Column("version", sa.String(128), nullable=False),
        sa.Column("artifact_uri", sa.Text(), nullable=False),
        sa.Column("artifact_sha256", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("source_job_id", name="uq_models_source_job"),
        sa.CheckConstraint(
            "artifact_sha256 ~ '^[0-9a-f]{64}$'", name="ck_models_artifact_sha256"
        ),
    )
    # MLP-010 already verified the bytes before writing these terminal results.
    # Use its evidence, leaving jobs without a verified result untouched.
    op.execute(
        sa.text("""
        WITH registered AS (
            INSERT INTO models (id, tenant_id, source_job_id, version,
                                artifact_uri, artifact_sha256)
            SELECT gen_random_uuid(), j.tenant_id, j.id, 'v1', j.artifact_uri,
                   a.execution_result ->> 'checksum'
            FROM jobs j
            JOIN job_attempts a ON a.job_id = j.id AND a.attempt_no = j.attempt
            WHERE j.status = 'succeeded'
              AND j.result_model_id IS NULL AND j.result_model_version IS NULL
              AND a.executor_kind = 'ray-training-v1'
              AND a.last_observation = 'succeeded' AND a.finished_at IS NOT NULL
              AND j.artifact_uri = a.execution_result ->> 'artifact_uri'
              AND j.artifact_uri LIKE 's3://%/model.pt'
              AND a.execution_result ->> 'checksum' ~ '^[0-9a-f]{64}$'
            RETURNING id, source_job_id, version
        )
        UPDATE jobs
        SET result_model_id = registered.id, result_model_version = registered.version,
            state_version = jobs.state_version + 1, updated_at = clock_timestamp()
        FROM registered
        WHERE jobs.id = registered.source_job_id
    """)
    )


def downgrade() -> None:
    if op.get_bind().scalar(sa.text("SELECT EXISTS (SELECT 1 FROM models)")):
        raise RuntimeError("Cannot downgrade while registered models exist")
    op.drop_table("models")
