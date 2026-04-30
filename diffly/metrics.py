# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import polars as pl
import polars.selectors as cs

Metric = Callable[[pl.Expr, pl.Expr], pl.Expr]
"""A metric is a callable mapping ``(left_expr, right_expr)`` to a scalar aggregation
expression.

The expressions refer to the left-side and right-side values of a single column across
all joined rows.
"""


@dataclass(frozen=True)
class _Metric:
    """A metric paired with a column-applicability selector.

    Internal only.
    """

    fn: Metric
    selector: pl.Expr


def _make_numeric_metric(metric: Metric) -> _Metric:
    return _Metric(fn=metric, selector=cs.numeric())


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


def quantile(q: float) -> Metric:
    """Factory returning a metric that computes the ``q``-quantile of
    ``right - left``."""

    def _quantile(left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return (right - left).quantile(q)

    return _quantile


_PRESETS: dict[str, Metric] = {
    "mean": mean,
    "median": median,
    "min": min,
    "max": max,
    "std": std,
    "mean_absolute_deviation": mean_absolute_deviation,
    "mean_relative_deviation": mean_relative_deviation,
}
