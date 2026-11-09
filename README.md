# KUSANAGI NERV

**草薙・迷走神経**  
*The Wandering Nerve of Distributed Intelligence*

A learning project for a self-service ML training and inference platform.
Start with the [project documentation](docs/README.md) and
[implementation roadmap](docs/03-planning/roadmap.md).

## Development setup

Install [uv](https://docs.astral.sh/uv/getting-started/installation/). The bootstrap
uses uv 0.13.7; the project interpreter is pinned in [.python-version](.python-version).
From the repository root:

```bash
uv python install
uv sync --locked
```

`uv` creates `.venv`, installs the package from `src/` and uses the checked-in
`uv.lock` for development dependencies. Virtual environment activation is optional
when using `uv run`.

## Checks

```bash
uv run --locked ruff check src tests
uv run --locked ruff format --check src tests
uv run --locked pytest -q
uv build
```

The [CI workflow](.github/workflows/ci.yml) runs these checks on pull requests to
`main` and pushes to `main`. Build artifacts are written to the ignored `dist/`
directory.

## Run the local job API

```bash
uv run --locked uvicorn kusanagi_nerv.api.app:create_app --factory --host 127.0.0.1 --port 8000
```

Open [interactive API docs](http://127.0.0.1:8000/docs), or submit a job:

```bash
curl -i http://127.0.0.1:8000/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"model":"tiny-mlp","dataset":"synthetic-v1","epochs":5,"resources":{"cpus":2,"gpus":0,"memory_gib":2},"max_retries":2}'
```

The response is `202` with a UUID, `status: "queued"` and a relative `Location`
such as `/v1/jobs/<job_id>`. Send a GET to that path on the same server to inspect
the job. The [API contract](docs/01-product/api-contracts.md) documents validation
limits and typed `404`/`422` errors.

This increment uses one fixed **test tenant**, with no authentication. Records are
in memory: use a single worker; restarting the app loses its jobs. Every POST
creates a new job, even with the same `Idempotency-Key`. No executor is connected,
so accepted jobs stay queued with `attempt: 0`; training, durable storage,
idempotency and cancellation arrive in later tasks.

## Implemented increments

- [MLP-001](docs/03-planning/tasks/MLP-001.md): eight-state pure Python
  [lifecycle](src/kusanagi_nerv/domain/job_lifecycle.py), merged with technical
  review approval; the learner's final checkpoint remains pending.
- [MLP-002](docs/03-planning/tasks/MLP-002.md): typed create/get API,
  [validation models](src/kusanagi_nerv/api/schemas.py), replaceable in-memory
  [repository](src/kusanagi_nerv/repositories/jobs.py) and
  [API contract tests](tests/api/test_jobs.py). In review.

The current suite has 146 tests. See the [progress journal](docs/03-planning/progress.md)
for local verification, hosted checks and scope. Static analysis with mypy/Pyright
remains a P1 follow-up; runtime schema validation and Ruff are enforced now.
