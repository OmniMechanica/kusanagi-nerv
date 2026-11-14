"""Private authentication environment for an owned local Ray runtime."""

import os
import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def runtime_environment(directory: str) -> Iterator[None]:
    """Use an ephemeral token, without reading or changing a user's token file."""
    token_path = Path(directory) / "auth_token"
    with open(
        token_path, "x", opener=lambda path, flags: os.open(path, flags, 0o600)
    ) as f:
        f.write(secrets.token_hex(32))
    names = (
        "RAY_AUTH_MODE",
        "RAY_AUTH_TOKEN",
        "RAY_AUTH_TOKEN_PATH",
        "RAY_USAGE_STATS_ENABLED",
        "RAY_DEDUP_LOGS",
    )
    previous = {name: os.environ.get(name) for name in names}
    try:
        os.environ.pop("RAY_AUTH_TOKEN", None)
        os.environ.update(
            RAY_AUTH_MODE="token",
            RAY_AUTH_TOKEN_PATH=str(token_path),
            RAY_USAGE_STATS_ENABLED="0",
            RAY_DEDUP_LOGS="0",
        )
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
