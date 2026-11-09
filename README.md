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

## First exercise: MLP-001

The pure Python job lifecycle is implemented and locally verified: eight states,
18 allowed transitions and explicit errors for forbidden transitions. The 79 tests
cover all 64 state pairs, lifecycle scenarios, argument types and package import.
MLP-001 is **In review**, with the learner's design explanation still pending.

- Task: [MLP-001](docs/03-planning/tasks/MLP-001.md).
- Contract: [job lifecycle](docs/01-product/job-lifecycle.md).
- Implementation: [job_lifecycle.py](src/kusanagi_nerv/domain/job_lifecycle.py).
- Tests: [test_job_lifecycle.py](tests/unit/test_job_lifecycle.py).

Read `JobStatus`, the immutable transition map, `can_transition()` and
`transition()` in that order. The [design notes](docs/01-product/job-lifecycle.md#mlp-001-implementation-notes)
explain the choices; the [progress journal](docs/03-planning/progress.md)
records verification and scope. These functions check graph rules; persistence,
execution attempts and safe remote cancellation arrive in later tasks.
