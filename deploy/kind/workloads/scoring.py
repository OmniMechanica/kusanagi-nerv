WEIGHTS = (0.5, -0.25, 0.75, 1.0)
MODEL_VERSION = "linear-cpu-v1"


def score(row: tuple[float, float, float, float]) -> float:
    return sum(feature * weight for feature, weight in zip(row, WEIGHTS, strict=True))
