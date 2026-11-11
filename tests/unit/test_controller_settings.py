"""Invalid timing must fail before starting the controller loop."""

import pytest

from kusanagi_nerv.controller.service import ControllerSettings


@pytest.mark.parametrize(
    "values",
    [
        {"lease_seconds": 0},
        {"lease_seconds": float("inf")},
        {"lease_seconds": 3601},
        {"poll_seconds": -1},
        {"poll_seconds": float("nan")},
        {"executor_timeout_seconds": 0},
        {"executor_timeout_seconds": float("inf")},
        {"poll_seconds": 10},
        {"executor_timeout_seconds": 10},
    ],
)
def test_invalid_timing_is_rejected(values: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        ControllerSettings(**values)
