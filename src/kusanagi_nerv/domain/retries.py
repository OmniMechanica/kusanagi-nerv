"""Bounded exponential backoff with equal jitter, sampled once per failed attempt."""

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    base_seconds: float = 1
    cap_seconds: float = 60

    def __post_init__(self) -> None:
        if (
            not all(
                math.isfinite(v) and v > 0
                for v in (self.base_seconds, self.cap_seconds)
            )
            or self.base_seconds > self.cap_seconds
        ):
            raise ValueError("Retry bounds must be finite, positive and base <= cap")

    def delay(self, attempt_no: int, jitter: float) -> float:
        if attempt_no < 1 or not math.isfinite(jitter) or not 0 <= jitter <= 1:
            raise ValueError("Invalid attempt number or jitter sample")
        ceiling = self.base_seconds
        for _ in range(attempt_no - 1):
            if ceiling >= self.cap_seconds:
                break
            ceiling = min(self.cap_seconds, ceiling * 2)
        return ceiling * (0.5 + 0.5 * jitter)
