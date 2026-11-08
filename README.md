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

Bootstrap is in place. The single smoke test verifies that the installed package
can be imported from outside the checkout. **The job state machine and its domain
tests are still to be implemented by the developer.**

- Task: [MLP-001](docs/03-planning/tasks/MLP-001.md).
- Contract: [job lifecycle](docs/01-product/job-lifecycle.md).
- Implementation: [job_lifecycle.py](src/kusanagi_nerv/domain/job_lifecycle.py).
- Tests: [test_job_lifecycle.py](tests/unit/test_job_lifecycle.py).

Start with `JobStatus` and `can_transition()`, then add the transition exception
and `transition()`. Record review and verification in the
[progress journal](docs/03-planning/progress.md).
