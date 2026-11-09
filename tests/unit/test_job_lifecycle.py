"""Check the public lifecycle contract against independently specified edges."""

from itertools import product

import pytest

from kusanagi_nerv.domain.job_lifecycle import (
    InvalidJobTransition,
    JobStatus,
    can_transition,
    transition,
)

# Source: docs/01-product/job-lifecycle.md, not the implementation's private map.
EXPECTED_EDGES = {
    ("created", "queued"),
    ("created", "cancelled"),
    ("queued", "running"),
    ("queued", "succeeded"),
    ("queued", "retrying"),
    ("queued", "failed"),
    ("queued", "cancelling"),
    ("queued", "cancelled"),
    ("running", "succeeded"),
    ("running", "retrying"),
    ("running", "failed"),
    ("running", "cancelling"),
    ("retrying", "queued"),
    ("retrying", "failed"),
    ("retrying", "cancelled"),
    ("cancelling", "cancelled"),
    ("cancelling", "failed"),
    ("cancelling", "succeeded"),
}


def test_status_names_and_serialized_values() -> None:
    assert {name: status.value for name, status in JobStatus.__members__.items()} == {
        "CREATED": "created",
        "QUEUED": "queued",
        "RUNNING": "running",
        "RETRYING": "retrying",
        "CANCELLING": "cancelling",
        "SUCCEEDED": "succeeded",
        "FAILED": "failed",
        "CANCELLED": "cancelled",
    }


@pytest.mark.parametrize(("current", "new"), list(product(JobStatus, repeat=2)))
def test_all_state_pairs(current: JobStatus, new: JobStatus) -> None:
    expected = (current.value, new.value) in EXPECTED_EDGES
    assert can_transition(current, new) is expected

    if expected:
        assert transition(current, new) is new
    else:
        with pytest.raises(InvalidJobTransition) as caught:
            transition(current, new)
        assert caught.value.current is current
        assert caught.value.new is new
        assert str(caught.value) == (
            f"Cannot transition job from '{current.value}' to '{new.value}'."
        )


@pytest.mark.parametrize(
    "path",
    [
        pytest.param(
            ("created", "queued", "running", "succeeded"),
            id="normal-completion",
        ),
        pytest.param(
            (
                "created",
                "queued",
                "running",
                "retrying",
                "queued",
                "running",
                "succeeded",
            ),
            id="transient-failure-then-success",
        ),
        pytest.param(
            ("created", "queued", "running", "cancelling", "succeeded"),
            id="success-observed-after-cancellation-intent",
        ),
        pytest.param(
            ("created", "queued", "failed"),
            id="confirmed-failure-before-running-observation",
        ),
        pytest.param(
            ("created", "queued", "succeeded"),
            id="short-job-completes-between-polls",
        ),
        pytest.param(
            ("created", "queued", "running", "cancelling", "cancelled"),
            id="confirmed-stop-after-cancellation-intent",
        ),
    ],
)
def test_lifecycle_scenarios(path: tuple[str, ...]) -> None:
    """Walk observed states; this does not simulate an executor or concurrency."""
    current = JobStatus(path[0])
    for value in path[1:]:
        current = transition(current, JobStatus(value))
    assert current is JobStatus(path[-1])


@pytest.mark.parametrize(
    ("current", "new"),
    [
        ("created", JobStatus.QUEUED),
        (JobStatus.CREATED, "queued"),
        ("created", "queued"),
        (None, JobStatus.QUEUED),
        (JobStatus.CREATED, None),
        (0, JobStatus.QUEUED),
        (JobStatus.CREATED, 0),
    ],
)
def test_non_status_arguments_are_rejected(current: object, new: object) -> None:
    with pytest.raises(TypeError, match="current and new must be JobStatus values"):
        can_transition(current, new)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="current and new must be JobStatus values"):
        transition(current, new)  # type: ignore[arg-type]
