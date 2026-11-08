# Integrating documentation into the existing repository

## Current baseline and intent

Inspection on 2026-11-08 found only README/LICENSE plus the added `docs/` tree; the separate application starter had not been imported. Begin with [repository bootstrap](bootstrap.md) before MLP-001. Recheck the filesystem before acting: this is a dated observation, not a permanent assumption.

This package contains only a `docs/` tree. It is designed to be unzipped **into the existing KUSANAGI NERV repository root** without replacing Python source, CI, compose, `pyproject.toml` or the existing README.

```text
kusanagi-nerv/                      # existing repository root
├── docs/                            # add from this package
│   ├── README.md
│   ├── 00-foundation/
│   ├── 01-product/
│   ├── 02-architecture/
│   ├── 03-planning/
│   ├── 04-learning/
│   ├── 05-interview-prep/
│   ├── 06-engineering/
│   └── 07-reference/
├── src/                             # optional starter code; absent at baseline
├── tests/                           # optional starter tests; absent at baseline
├── pyproject.toml                   # optional starter config; absent at baseline
└── README.md                        # existing root README, untouched
```

## If the original `mini-ml-platform` starter was already imported

The starter uses Python package `ml_platform`, `src/ml_platform/...`, `challenges/` and some flat `docs/*.md` files. **Keep working code and original tests intact** initially. The documentation here is the new organized canonical plan; reconcile or archive duplicates only after reviewing content in git. Legacy challenge tests must be reviewed against the current lifecycle specification before reuse; the pending-dispatch cases have been clarified since the starter was prepared.

Suggested future maintenance (outside this docs-only package):

1. Update the root README title, branding and links to `docs/README.md`.
2. Update `pyproject.toml` distribution name if still `mini-ml-platform`; choose a stable Python package path separately.
3. Only rename the import package if tests, entrypoints, CI, Docker files and absolute module references are updated together.
4. Keep `MLP-001` … `MLP-026` stable in tickets and commit messages for backward compatibility.
5. Use a separate commit for the renaming refactor; avoid combining it with the Day 1 state machine change.

## Safe verification after adding docs

```bash
cd /Users/ykostiannikov/workspace/kusanagi-nerv
git status --short
find docs -maxdepth 3 -type f | sort
# When ready:
git add docs
git commit -m 'docs: add KUSANAGI NERV capstone roadmap'
```

`git commit` is optional; inspect generated/added files before committing. Never assume your local repository was changed until you run installation and verify `git status`.
