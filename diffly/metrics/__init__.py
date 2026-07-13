# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

"""Metrics computed per column when generating a summary.

Two families are provided:

- Metrics in :mod:`~diffly.metrics.change` describe the change between numeric
  columns itself by aggregating over ``right - left``.
- Metrics in :mod:`~diffly.metrics.data` describe the left and right datasets
  individually, explaining how a change affects the data.
"""

from __future__ import annotations

from . import change, data
from ._common import Metric, MetricFn
from .change import (
    _make_numeric_metric,
    max,
    mean,
    mean_absolute_deviation,
    mean_relative_deviation,
    median,
    min,
    quantile,
    std,
)

DEFAULT_METRICS: dict[str, MetricFn | Metric] = {
    **change.DEFAULT_CHANGE_METRICS,
    **data.DEFAULT_DATA_METRICS,
}

__all__ = [
    "DEFAULT_METRICS",
    "Metric",
    "MetricFn",
    "change",
    "data",
    "max",
    "mean",
    "mean_absolute_deviation",
    "mean_relative_deviation",
    "median",
    "min",
    "quantile",
    "std",
    "_make_numeric_metric",
]
