"""Canonical admitted specifications and stable, tenant-scoped request keys."""

import hashlib
import json
import re
from dataclasses import asdict
from typing import Any

from kusanagi_nerv.domain.jobs import JobSpecification


def validate_key(key: str | None) -> None:
    if key is not None and not re.fullmatch(r"[!-~]{1,255}", key):
        raise ValueError("Idempotency key must contain 1-255 visible ASCII characters")


def canonical_spec(spec: JobSpecification) -> tuple[dict[str, Any], str]:
    payload = asdict(spec)
    payload["resources"]["cpus"] = float(spec.resources.cpus)
    payload["resources"]["memory_gib"] = float(spec.resources.memory_gib)
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return payload, hashlib.sha256(encoded.encode("utf-8")).hexdigest()
