# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

"""Metrics computed per column when generating a summary.

Two families are provided:

- :mod:`~diffly.metrics.change` describes the change between numeric columns by
  aggregating over ``right - left``.
- :mod:`~diffly.metrics.data` describes the left and right datasets individually,
  so you can understand how a change affects the data.
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
from .data import null_fraction_change

DEFAULT_METRICS: dict[str, MetricFn | Metric] = {
    **change.DEFAULT_METRICS,
    **data.DEFAULT_METRICS,
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
    "null_fraction_change",
    "quantile",
    "std",
    "_make_numeric_metric",
]
