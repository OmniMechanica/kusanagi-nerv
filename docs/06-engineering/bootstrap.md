# Repository bootstrap before MLP-001

**Scope:** preparation within MLP-001, not a new ticket. Allow an additional 30–60 minutes, separate from its 60–90-minute domain exercise. This page is an instruction, not evidence that setup has already run.

**Current status:** bootstrap has been implemented and verified locally. Exact versions and observed results are recorded in [progress](../03-planning/progress.md#2026-11-08--mlp-001--repository-bootstrap). The lifecycle module and its unit-test file remain exercise placeholders; the passing test covers package installation only.

## Starting point

The repository inspection on 2026-11-08 found the root README, LICENSE and this documentation; no application starter, `pyproject.toml`, `src/` or `tests/` was present. Inspect again before creating files. If a starter has since been imported, preserve its package and tests and follow [repository integration](repository-integration.md).

## Initial choices

- Initial interpreter target: CPython 3.12. Record the interpreter in `.python-version` and compatible `requires-python` metadata; record the exact version used in evidence. This is a Day 1 baseline, not a claim that the full ML stack has been validated on this machine.
- Distribution name: `kusanagi-nerv`; import package: `kusanagi_nerv` for a fresh repository. An existing starter using `ml_platform` should retain that package until a separate, tested rename.
- Use a packaged `src/` layout, `uv` for the environment/lockfile, and pytest/Ruff as development dependencies.
- Add Ray, PyTorch, FastAPI and database libraries only at their milestones. Before MLP-006/008, verify and pin compatible runtime versions for the actual OS/architecture.

## Preparation steps

1. Inspect `git status --short` and existing configuration. Install `uv` from the [official installation instructions](https://docs.astral.sh/uv/getting-started/installation/) if missing; record `uv --version`.
2. Create or adapt the package configuration using the [uv package project guide](https://docs.astral.sh/uv/concepts/projects/init/). Preserve the README and LICENSE. Ensure the build backend installs the package from `src/`.
3. Pin the selected interpreter and add the development tools:

   ```bash
   uv python pin 3.12
   uv add --dev pytest ruff
   ```

4. Create the domain/test directories below. The developer implements the lifecycle exercise; bootstrap does not supply its solution.
5. Configure pytest to collect `tests/`; configure Ruff's Python target consistently with the interpreter baseline. Ignore `.venv/`, Python/test/lint caches and build artifacts. Keep `uv.lock` under version control when the implementation is committed.
6. Add a minimal CI check with the same Python/uv versions, locked dependency installation, lint, formatting and unit tests. Extend it with service integration tests as those services arrive; MLP-019 reviews and hardens that pipeline.

Bootstrap layout:

```text
pyproject.toml
.python-version
uv.lock
.gitignore
src/kusanagi_nerv/__init__.py
src/kusanagi_nerv/domain/__init__.py
src/kusanagi_nerv/domain/job_lifecycle.py
tests/unit/test_job_lifecycle.py
tests/smoke/test_package_installation.py
.github/workflows/ci.yml
```

## Verification and handoff

After the package exists, verify installation/import. After the first lifecycle tests are written, run all checks:

```bash
uv sync --locked
uv run python --version
uv run python -c 'import kusanagi_nerv'
uv run pytest -q
uv run ruff check src tests
uv run ruff format --check src tests
```

Do not count zero collected domain tests or a passing package smoke test as completion of MLP-001. A workflow file is configuration evidence; claim a successful CI run only after observing one. Record commands, interpreter/tool versions and results in [progress](../03-planning/progress.md); mark MLP-001 Done only after its acceptance criteria pass.

Continue with [MLP-001](../03-planning/tasks/MLP-001.md). No database, Ray cluster, Kubernetes or GPU is required for this preparation.
