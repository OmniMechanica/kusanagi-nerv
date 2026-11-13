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


def test_ray_example_help_works_without_starting_ray(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-I", "-m", "kusanagi_nerv.examples.ray_core", "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "--max-outstanding" in result.stdout
    assert "RAY_DEMO_CLEANUP" not in result.stdout


def test_api_import_does_not_load_optional_ray_runtime(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "import kusanagi_nerv.api.app; import sys; assert 'ray' not in sys.modules",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
