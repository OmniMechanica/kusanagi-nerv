#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

docker=(docker)
if [[ -n "${KUSANAGI_DOCKER_CONTEXT:-}" ]]; then
    docker+=(--context "$KUSANAGI_DOCKER_CONTEXT")
fi
"${docker[@]}" compose up -d --wait postgres s3

if [[ -z "${KUSANAGI_TEST_DATABASE_URL:-}" ]]; then
    postgres_address=$("${docker[@]}" compose port postgres 5432)
    KUSANAGI_TEST_DATABASE_URL="postgresql+asyncpg://kusanagi:local-development-only@$postgres_address/postgres"
fi
if [[ -z "${KUSANAGI_S3_ENDPOINT:-}" ]]; then
    s3_address=$("${docker[@]}" compose port s3 8333)
    KUSANAGI_S3_ENDPOINT="http://$s3_address"
fi
export KUSANAGI_TEST_DATABASE_URL KUSANAGI_S3_ENDPOINT
export KUSANAGI_S3_BUCKET="${KUSANAGI_S3_BUCKET:-kusanagi-training}"
export KUSANAGI_S3_ACCESS_KEY="${KUSANAGI_S3_ACCESS_KEY:-kusanagi-local}"
export KUSANAGI_S3_SECRET_KEY="${KUSANAGI_S3_SECRET_KEY:-local-development-only}"

environments="$(mktemp -d "${TMPDIR:-/tmp}/kusanagi-cpu.XXXXXXXX")"
trap 'rm -rf -- "$environments"' EXIT

if [[ "$(uname -s)" == Darwin ]]; then
    UV_PROJECT_ENVIRONMENT="$environments/recovery" uv sync --locked --extra recovery
    UV_PROJECT_ENVIRONMENT="$environments/serving" uv sync --locked --extra serving
    export KUSANAGI_TEST_SERVING_PYTHON="$environments/serving/bin/python"
else
    UV_PROJECT_ENVIRONMENT="$environments/recovery" uv sync --locked --extra recovery --extra serving
    export KUSANAGI_TEST_SERVING_PYTHON="$environments/recovery/bin/python"
fi

"$environments/recovery/bin/python" -m pytest -q -s tests/integration/test_cpu_pipeline.py "$@"
