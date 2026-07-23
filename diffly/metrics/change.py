# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause
"""Metrics describing the change between numeric columns.

These aggregate over ``right - left`` to characterize the change itself.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

import polars as pl
import polars.selectors as cs

ChangeMetricFn = Callable[[pl.Expr, pl.Expr], pl.Expr]
"""A change metric maps the difference between two joined columns from the compared data
frames to a scalar aggregation expression."""


@dataclass(frozen=True)
class ChangeMetric:
    """A metric quantifying the *change* between the two sides of a comparison.

    Change metrics are rendered as extra columns in the "Columns" table, alongside the
    match rate.
    """

    fn: ChangeMetricFn
    """Aggregates over ``right - left`` (e.g. the mean delta) to describe the change
    itself."""

    selector: cs.Selector = field(default_factory=cs.numeric)
    """Selects the columns the metric applies to; defaults to numeric columns."""


def mean(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Mean of ``right - left``."""
    return (right - left).mean()


def median(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Median of ``right - left``."""
    return (right - left).median()


def min(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Minimum of ``right - left``."""
    return (right - left).min()


def max(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Maximum of ``right - left``."""
    return (right - left).max()


def std(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Standard deviation of ``right - left``."""
    return (right - left).std()


def mean_absolute_deviation(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Mean of ``|right - left|``."""
    return (right - left).abs().mean()


def mean_relative_deviation(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Mean of ``|(right - left) / left|``. Yields ``inf`` or ``null`` where
    ``left`` is zero."""
    return ((right - left) / left).abs().mean()


def quantile(q: float) -> ChangeMetricFn:
    """Factory returning a metric that computes the ``q``-quantile of
    ``right - left``."""
    if not 0 <= q <= 1:
        raise ValueError(f"q must be in [0, 1], got {q}")

    def _quantile(left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return (right - left).quantile(q)

    return _quantile


DEFAULT_CHANGE_METRICS: dict[str, ChangeMetric] = {
    "Mean": ChangeMetric(fn=mean),
    "Median": ChangeMetric(fn=median),
    "Min": ChangeMetric(fn=min),
    "Max": ChangeMetric(fn=max),
    "Std": ChangeMetric(fn=std),
    "Mean absolute deviation": ChangeMetric(fn=mean_absolute_deviation),
    "Mean relative deviation": ChangeMetric(fn=mean_relative_deviation),
}
"""Preset metrics describing the change between numeric columns."""
