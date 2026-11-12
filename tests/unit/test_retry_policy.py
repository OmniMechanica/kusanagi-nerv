"""Retry jitter bounds are deterministic; the selected timestamp is stored in DB."""

import pytest

from kusanagi_nerv.domain.retries import RetryPolicy


@pytest.mark.parametrize(
    ("attempt", "sample", "expected"),
    [
        (1, 0, 0.5),
        (1, 1, 1),
        (2, 0, 1),
        (2, 1, 2),
        (3, 0.5, 3),
        (4, 1, 8),
        (10, 0, 4),
        (1000, 1, 8),
    ],
)
def test_exponential_delay_equal_jitter_and_cap(
    attempt: int, sample: float, expected: float
) -> None:
    assert RetryPolicy(cap_seconds=8).delay(attempt, sample) == expected


@pytest.mark.parametrize(
    ("attempt", "sample"), [(0, 0), (1, -0.1), (1, 1.1), (1, float("nan"))]
)
def test_invalid_retry_sample_is_rejected(attempt: int, sample: float) -> None:
    with pytest.raises(ValueError):
        RetryPolicy().delay(attempt, sample)
