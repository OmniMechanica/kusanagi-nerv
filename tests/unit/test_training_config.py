"""Input validation before importing the optional training dependency."""

import pytest

from kusanagi_nerv.training.config import TrainingConfig


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("seed", -1),
        ("seed", True),
        ("epochs", 0),
        ("epochs", 201),
        ("batch_size", 0),
        ("batch_size", 385),
        ("learning_rate", float("nan")),
        ("learning_rate", float("inf")),
        ("learning_rate", 0),
    ],
)
def test_invalid_training_options(name: str, value: object) -> None:
    with pytest.raises(ValueError, match=name):
        TrainingConfig(**{name: value})
