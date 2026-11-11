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
uv run --locked ruff check src tests migrations
uv run --locked ruff format --check src tests migrations
uv run --locked pytest -q -m 'not integration'
uv build
```

The [CI workflow](.github/workflows/ci.yml) runs fast checks and a separate real
PostgreSQL integration job on all pull requests and pushes to `main`. Build
artifacts are written to the ignored `dist/` directory. See the
[PostgreSQL guide](docs/06-engineering/postgres.md#run-the-checks) for database tests.

## Run the local job API

```bash
docker compose up -d --wait postgres
export KUSANAGI_DATABASE_URL='postgresql+asyncpg://kusanagi:local-development-only@127.0.0.1:55432/kusanagi'
uv run --locked alembic upgrade head
uv run --locked uvicorn kusanagi_nerv.api.app:create_app --factory --host 127.0.0.1 --port 8000
```

Docker must be running. The database uses loopback port 55432 and a persistent
named volume. The credentials above are public local-development examples.
The API requires its database URL and never silently falls back to memory.

Open [interactive API docs](http://127.0.0.1:8000/docs), or submit a job:

```bash
curl -i http://127.0.0.1:8000/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"model":"tiny-mlp","dataset":"synthetic-v1","epochs":5,"resources":{"cpus":2,"gpus":0,"memory_gib":2},"max_retries":2}'
```

The response is `202` with a UUID, `status: "queued"` and a relative `Location`
such as `/v1/jobs/<job_id>`. Send a GET to that path on the same server to inspect
the job. The [API contract](docs/01-product/api-contracts.md) documents validation
limits and typed `404`/`422`/`503` errors.

This increment uses one fixed **test tenant**, with no authentication. Jobs are
committed to PostgreSQL before acceptance and survive API restarts. Every POST
still creates a new job, even with the same `Idempotency-Key`. Without a controller,
accepted jobs stay queued with `attempt: 0`. See the [PostgreSQL guide](docs/06-engineering/postgres.md)
for migration, concurrency and failure semantics, including uncertain commits.

## Run the local controller simulation

In another terminal, using the same migrated database:

```bash
export KUSANAGI_DATABASE_URL='postgresql+asyncpg://kusanagi:local-development-only@127.0.0.1:55432/kusanagi'
uv run --locked python -m kusanagi_nerv.controller --executor simulated
```

CPU requests progress through one persisted attempt to simulated success. **No
model training or artifact creation occurs.** GPU requests stay queued without an
attempt; there is no CPU fallback. A second controller can run in another terminal:
claims are coordinated in PostgreSQL. After a controller crash, recovery waits for
its lease to expire (30 seconds by default), then reconciles the same submission.
Read the [controller guide](docs/06-engineering/controller.md) for ownership,
failure behavior, timing, limits and reproducible tests. Idempotency, retries and
cancellation follow in MLP-005; Ray and real training follow later.

## Implemented increments

- [MLP-001](docs/03-planning/tasks/MLP-001.md): eight-state pure Python
  [lifecycle](src/kusanagi_nerv/domain/job_lifecycle.py), merged with technical
  review approval; the learner's final checkpoint remains pending.
- [MLP-002](docs/03-planning/tasks/MLP-002.md): typed create/get API,
  [validation models](src/kusanagi_nerv/api/schemas.py), replaceable in-memory
  [repository](src/kusanagi_nerv/repositories/jobs.py) and
  [API contract tests](tests/api/test_jobs.py). PR #5 merged; learning drill pending.
- [MLP-003](docs/03-planning/tasks/MLP-003.md): transactional PostgreSQL
  [repository](src/kusanagi_nerv/repositories/postgres.py), explicit Alembic migration,
  version-guarded state updates and real process-crash/concurrency tests. PR #6 merged
  into the MLP-002 branch; carried into this branch for integration into `main`.
- [MLP-004](docs/03-planning/tasks/MLP-004.md): separate
  [controller](src/kusanagi_nerv/controller/service.py), durable attempts,
  fenced leases and a persistent simulated executor. In review; learning drill pending.

The full suite has 204 tests: 165 fast cases and 39 PostgreSQL integration cases.
See the [progress journal](docs/03-planning/progress.md)
for local verification, hosted checks and scope. Static analysis with mypy/Pyright
remains a P1 follow-up; runtime schema validation and Ruff are enforced now.
