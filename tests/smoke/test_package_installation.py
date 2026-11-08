"""Verify the bootstrap installs the src-layout package into the environment."""

import subprocess
import sys
from pathlib import Path


def test_installed_package_imports_outside_checkout(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-I", "-c", "import kusanagi_nerv.domain.job_lifecycle"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 0, result.stderr
