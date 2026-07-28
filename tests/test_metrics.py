# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import math
from typing import Any

import polars as pl
import pytest

from diffly import metrics
from diffly.comparison import _resolve_change_metric, _resolve_data_metric
from diffly.metrics import data
from diffly.metrics.change import ChangeMetric, ChangeMetricFn
from diffly.metrics.data import DataMetric


@pytest.fixture
def frame() -> pl.DataFrame:
    # deltas (right - left): [0, 0, 2, null]
    return pl.DataFrame({"l": [1, 2, 3, None], "r": [1, 2, 5, 4]})


def _apply(metric: ChangeMetricFn, frame: pl.DataFrame) -> Any:
    return frame.select(metric(pl.col("l"), pl.col("r"))).item()


def test_mean(frame: pl.DataFrame) -> None:
    assert _apply(metrics.change.mean, frame) == pytest.approx(2 / 3)


def test_median(frame: pl.DataFrame) -> None:
    assert _apply(metrics.change.median, frame) == 0


def test_min(frame: pl.DataFrame) -> None:
    assert _apply(metrics.change.min, frame) == 0


def test_max(frame: pl.DataFrame) -> None:
    assert _apply(metrics.change.max, frame) == 2


def test_std(frame: pl.DataFrame) -> None:
    sample_mean = 2 / 3
    assert _apply(metrics.change.std, frame) == pytest.approx(
        math.sqrt(
            ((0 - sample_mean) ** 2 + (0 - sample_mean) ** 2 + (2 - sample_mean) ** 2)
            / 2
        )
    )


def test_mean_absolute_deviation() -> None:
    # deltas: [-1, 0, 2, null]; |deltas|: [1, 0, 2, null]; mean = 1.0
    frame = pl.DataFrame({"l": [2, 2, 3, None], "r": [1, 2, 5, 4]})
    assert _apply(metrics.change.mean_absolute_deviation, frame) == pytest.approx(1.0)


def test_mean_relative_deviation() -> None:
    # left: [1, 2, 4, None]; delta: [0, 0, 2, null]; rel: [0, 0, 0.5, null]; mean = 1/6
    frame = pl.DataFrame({"l": [1, 2, 4, None], "r": [1, 2, 6, 4]})
    assert _apply(metrics.change.mean_relative_deviation, frame) == pytest.approx(1 / 6)


def test_mean_relative_deviation_div_by_zero() -> None:
    # Matches numpy: x/0 -> inf, so .abs().mean() -> inf
    frame = pl.DataFrame({"l": [0.0, 1.0], "r": [1.0, 1.0]})
    assert math.isinf(_apply(metrics.change.mean_relative_deviation, frame))


def test_null_fraction() -> None:
    # A data metric describes a single side: 1 null out of 4 rows.
    frame = pl.DataFrame({"l": [1, None, 3, 4]})
    assert frame.select(data.null_fraction(pl.col("l"))).item() == pytest.approx(0.25)


def test_null_fraction_non_numeric() -> None:
    # Applies to any column type; here strings. 1 null out of 2 rows.
    frame = pl.DataFrame({"l": ["a", None]})
    assert frame.select(data.null_fraction(pl.col("l"))).item() == pytest.approx(0.5)


def test_quantile(frame: pl.DataFrame) -> None:
    # deltas [0, 0, 2]: p50 = 0, p100 = 2
    assert _apply(metrics.change.quantile(0.5), frame) == 0
    assert _apply(metrics.change.quantile(1.0), frame) == 2


def test_quantile_out_of_range() -> None:
    with pytest.raises(ValueError, match="q must be in"):
        metrics.change.quantile(1.5)


def test_default_metrics_partition() -> None:
    from diffly.metrics import change

    # The change and data preset sets are disjoint.
    assert set(change.DEFAULT_CHANGE_METRICS).isdisjoint(set(data.DEFAULT_DATA_METRICS))


def test_resolve_data_metric_passthrough() -> None:
    metric = DataMetric(fn=data.null_fraction)
    assert _resolve_data_metric(metric) is metric


def test_resolve_change_metric_passthrough() -> None:
    metric = ChangeMetric(fn=metrics.change.mean)
    assert _resolve_change_metric(metric) is metric


def test_resolve_data_metric_wraps_callable() -> None:
    fn = data.null_fraction
    resolved = _resolve_data_metric(fn)
    assert isinstance(resolved, DataMetric)
    assert resolved.fn is fn


def test_resolve_change_metric_wraps_callable() -> None:
    fn = metrics.change.mean
    resolved = _resolve_change_metric(fn)
    assert isinstance(resolved, ChangeMetric)
    assert resolved.fn is fn
