# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import math
from typing import Any

import polars as pl
import pytest

from diffly import metrics
from diffly.metrics import MetricFn, data


@pytest.fixture
def frame() -> pl.DataFrame:
    # deltas (right - left): [0, 0, 2, null]
    return pl.DataFrame({"l": [1, 2, 3, None], "r": [1, 2, 5, 4]})


def _apply(metric: MetricFn, frame: pl.DataFrame) -> Any:
    return frame.select(metric(pl.col("l"), pl.col("r"))).item()


def test_mean(frame: pl.DataFrame) -> None:
    assert _apply(metrics.mean, frame) == pytest.approx(2 / 3)


def test_median(frame: pl.DataFrame) -> None:
    assert _apply(metrics.median, frame) == 0


def test_min(frame: pl.DataFrame) -> None:
    assert _apply(metrics.min, frame) == 0


def test_max(frame: pl.DataFrame) -> None:
    assert _apply(metrics.max, frame) == 2


def test_std(frame: pl.DataFrame) -> None:
    sample_mean = 2 / 3
    assert _apply(metrics.std, frame) == pytest.approx(
        math.sqrt(
            ((0 - sample_mean) ** 2 + (0 - sample_mean) ** 2 + (2 - sample_mean) ** 2)
            / 2
        )
    )


def test_mean_absolute_deviation() -> None:
    # deltas: [-1, 0, 2, null]; |deltas|: [1, 0, 2, null]; mean = 1.0
    frame = pl.DataFrame({"l": [2, 2, 3, None], "r": [1, 2, 5, 4]})
    assert _apply(metrics.mean_absolute_deviation, frame) == pytest.approx(1.0)


def test_mean_relative_deviation() -> None:
    # left: [1, 2, 4, None]; delta: [0, 0, 2, null]; rel: [0, 0, 0.5, null]; mean = 1/6
    frame = pl.DataFrame({"l": [1, 2, 4, None], "r": [1, 2, 6, 4]})
    assert _apply(metrics.mean_relative_deviation, frame) == pytest.approx(1 / 6)


def test_mean_relative_deviation_div_by_zero() -> None:
    # Matches numpy: x/0 -> inf, so .abs().mean() -> inf
    frame = pl.DataFrame({"l": [0.0, 1.0], "r": [1.0, 1.0]})
    assert math.isinf(_apply(metrics.mean_relative_deviation, frame))


def test_null_fraction_data() -> None:
    # left nulls: 1/4 = 25%; right nulls: 3/4 = 75%; delta = +50%
    frame = pl.DataFrame({"l": [1, None, 3, 4], "r": [None, None, 3, None]})
    assert _apply(data.null_fraction_data, frame) == "25.0% -> 75.0% (+50.0)"


def test_null_fraction_data_negative_delta() -> None:
    # left nulls: 1/2 = 50%; right nulls: 0%; delta = -50%
    frame = pl.DataFrame({"l": [1, None], "r": [1, 2]})
    assert _apply(data.null_fraction_data, frame) == "50.0% -> 0.0% (-50.0)"


def test_null_fraction_data_non_numeric() -> None:
    # Applies to any column type; here strings. left nulls: 0%; right nulls: 50%
    frame = pl.DataFrame({"l": ["a", "b"], "r": ["a", None]})
    assert _apply(data.null_fraction_data, frame) == "0.0% -> 50.0% (+50.0)"


def test_mean_data() -> None:
    # left mean: 2.0; right mean: 4.0; delta = +2.0
    frame = pl.DataFrame({"l": [1, 2, 3], "r": [3, 4, 5]})
    assert _apply(data.mean_data, frame) == "2.0 -> 4.0 (+2.0)"


def test_median_data() -> None:
    # left median: 2.0; right median: 4.0; delta = +2.0
    frame = pl.DataFrame({"l": [1, 2, 3], "r": [3, 4, 5]})
    assert _apply(data.median_data, frame) == "2.0 -> 4.0 (+2.0)"


def test_min_data() -> None:
    # left min: 1; right min: 3; delta = +2
    frame = pl.DataFrame({"l": [1, 2, 3], "r": [3, 4, 5]})
    assert _apply(data.min_data, frame) == "1 -> 3 (+2)"


def test_max_data() -> None:
    # left max: 5; right max: 3; delta = -2
    frame = pl.DataFrame({"l": [1, 2, 5], "r": [1, 2, 3]})
    assert _apply(data.max_data, frame) == "5 -> 3 (-2)"


def test_std_data() -> None:
    # both sides equal spread; delta = 0.0
    frame = pl.DataFrame({"l": [1.0, 2.0, 3.0], "r": [4.0, 5.0, 6.0]})
    assert _apply(data.std_data, frame) == "1.0 -> 1.0 (+0.0)"


def test_quantile(frame: pl.DataFrame) -> None:
    # deltas [0, 0, 2]: p50 = 0, p100 = 2
    assert _apply(metrics.quantile(0.5), frame) == 0
    assert _apply(metrics.quantile(1.0), frame) == 2


def test_quantile_out_of_range() -> None:
    with pytest.raises(ValueError, match="q must be in"):
        metrics.quantile(1.5)


def test_default_metrics_partition() -> None:
    from diffly.metrics import change

    # The top-level defaults consist of the change metrics only.
    assert metrics.DEFAULT_METRICS == {**change.DEFAULT_CHANGE_METRICS}
    assert set(change.DEFAULT_CHANGE_METRICS) & set(data.DEFAULT_DATA_METRICS) == set()
    assert list(metrics.DEFAULT_METRICS) == [*change.DEFAULT_CHANGE_METRICS]
