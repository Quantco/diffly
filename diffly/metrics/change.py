# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

"""Metrics describing the change between numeric columns.

These aggregate over ``right - left`` to characterize the change itself.
"""

from __future__ import annotations

import polars as pl
import polars.selectors as cs

from ._common import Metric, MetricFn


def _make_numeric_metric(fn: MetricFn) -> Metric:
    return Metric(fn=fn, selector=cs.numeric())


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


def quantile(q: float) -> MetricFn:
    """Factory returning a metric that computes the ``q``-quantile of
    ``right - left``."""
    if not 0 <= q <= 1:
        raise ValueError(f"q must be in [0, 1], got {q}")

    def _quantile(left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return (right - left).quantile(q)

    return _quantile


DEFAULT_METRICS: dict[str, MetricFn] = {
    "Mean": mean,
    "Median": median,
    "Min": min,
    "Max": max,
    "Std": std,
    "Mean absolute deviation": mean_absolute_deviation,
    "Mean relative deviation": mean_relative_deviation,
}
