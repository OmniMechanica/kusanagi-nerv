# PostgreSQL ledger: local setup and verification

MLP-003 makes PostgreSQL the source of truth for logical jobs. The HTTP contract creates queued jobs; MLP-004 adds an optional separate controller with simulated CPU execution. The API requires `KUSANAGI_DATABASE_URL` and never falls back silently to memory. Tests can explicitly inject `InMemoryJobRepository`.

## Start the local database and API

From the repository root, with Docker running:

```bash
uv sync --locked
docker compose up -d --wait postgres
export KUSANAGI_DATABASE_URL='postgresql+asyncpg://kusanagi:local-development-only@127.0.0.1:55432/kusanagi'
uv run --locked alembic upgrade head
uv run --locked alembic current
uv run --locked uvicorn kusanagi_nerv.api.app:create_app --factory --host 127.0.0.1 --port 8000
```

On a Mac whose active Docker context does not point to Docker Desktop, use `docker --context desktop-linux compose ...`; changing the global context is unnecessary. `compose.yaml` binds the database to loopback port 55432 and uses public local-development credentials. The named volume retains the database across container recreation. The pinned PostgreSQL 18 image mounts `/var/lib/postgresql`, as required by its [image layout](https://hub.docker.com/_/postgres/).

`.env.example` lists the development/test URLs. The application does not auto-load dotenv files. If you override `KUSANAGI_POSTGRES_PORT` for Compose, update both URLs to match.

Use the [README request example](../../README.md#run-the-local-job-api) to create a job. Stop and restart **the API process**, then GET the same `Location`: the record, timestamps and UUID remain unchanged. No `create_all`, reset, migration or table deletion runs at API startup. Run migrations explicitly before deploying a new schema.

Stop the development database while retaining its data:

```bash
docker compose stop postgres
```

## Run the checks

```bash
uv run --locked ruff check src tests migrations
uv run --locked ruff format --check src tests migrations
uv run --locked pytest -q -m 'not integration'
export KUSANAGI_TEST_DATABASE_URL='postgresql+asyncpg://kusanagi:local-development-only@127.0.0.1:55432/postgres'
uv run --locked pytest -q tests/integration
uv build
```

With the test URL exported, `uv run --locked pytest -q` runs the full suite. Without it, PostgreSQL integration cases are explicitly skipped, not counted as database evidence. CI runs fast checks and a separate PostgreSQL service job on pull requests, including dependent PRs.

The test role needs `CREATEDB`. Each integration case creates a randomly named `kusanagi_test_<uuid>` database, applies Alembic, then drops only that owned database. The database named in the supplied test URL is used for administration and is never reset. Tests include a real Uvicorn process killed after a successful response, followed by a fresh API process reading the persisted record.

`alembic check` detects schema/model drift. The downgrade/upgrade test operates only on its disposable database; a real downgrade to `base` deletes the ledger and is not a normal startup or cleanup step.

## Transactions and concurrent writers

Each repository operation owns a fresh `AsyncSession`. The context manager commits or rolls back and closes it; the application lifespan disposes its connection pool on shutdown. Sharing one mutable session between concurrent tasks is unsupported by [SQLAlchemy asyncio](https://docs.sqlalchemy.org/en/21/orm/extensions/asyncio.html#using-asyncsession-with-concurrent-tasks).

Creation stores a `QUEUED` job in one transaction, with `state_version = 0`, before `POST` returns `202`. There is no separate enqueue step that can strand a durable `CREATED` row. The stored canonical SHA-256 hash normalizes CPU/memory numbers and includes defaults supplied by validation. MLP-005 uses `idempotency_key` and the tenant/key unique constraint for keyed admission; unkeyed requests remain independent. A hash alone does not deduplicate requests. See [admission semantics](job-policies.md#request-identity).

The repository's internal `update_status()` validates the domain edge and performs a conditional update shaped like:

```sql
UPDATE jobs
SET status = :new_status,
    state_version = state_version + 1,
    updated_at = now()
WHERE id = :job_id AND tenant_id = :tenant_id
  AND status = :expected_status AND state_version = :expected_version
  AND attempt = 0
RETURNING *;
```

Zero returned rows raises `JobUpdateConflict`, including unknown/other-tenant IDs. Under PostgreSQL's default Read Committed isolation, a competing update waits and then rechecks the predicate against the committed row. The [PostgreSQL isolation documentation](https://www.postgresql.org/docs/18/transaction-iso.html#XACT-READ-COMMITTED) explains this behavior. Tests synchronize two readers with a barrier and verify exactly one successful version update; another case covers a retry cycle returning to the same status with a newer version.

This is a storage primitive, not execution authorization. It does not establish ownership of an attempt, revoke an in-flight submission or prove that remote work stopped. MLP-004 restricts this primitive to jobs without attempt history and adds [fenced attempt transactions](controller.md#ownership-and-external-calls). MLP-005 adds cancellation intent and runtime confirmation using the same job-first lock order.

## Failure behavior and remaining scope

- Database/driver failures become `503 storage_unavailable` without SQL, connection strings or raw driver messages in the response. An unsuccessful or unconfirmed admission never returns `202`.
- Connection/pool waits are bounded to 5 seconds; application commands/statements to 10 seconds, lock waits to 5 seconds. Migration connections use a 5-second connection timeout and 30-second command timeout. These are local learning defaults, not latency SLOs.
- A lost connection during commit can leave the outcome unknown; the server may have committed despite a failed response. There is no automatic write retry. Repeating an unkeyed POST may create another logical job. Repeating the same tenant/key/payload resolves that uncertainty through durable idempotency.
- `0002_attempt_leases` adds attempt history, fenced leases and simulated dispatch; see [the data model](../02-architecture/data-model.md#implemented-in-mlp-004). `0003_retry_cancel` adds retry deadlines, failure classification and cancellation metadata; actual model execution follows later.
- The fixed tenant is still test-only; production authentication and tenancy controls remain later milestones. Database persistence does not imply model training, backup/restore testing or high availability.
