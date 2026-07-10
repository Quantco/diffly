# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import polars as pl
import polars.selectors as cs


@dataclass(frozen=True)
class Metric:
    """A metric function paired with a column-applicability selector."""

    fn: MetricFn
    selector: cs.Selector


MetricFn = Callable[[pl.Expr, pl.Expr], pl.Expr]
"""A metric function maps ``(left_expr, right_expr)`` to a scalar aggregation
expression.

The expressions refer to the left-side and right-side values of a single column across
all joined rows.
"""


def _make_numeric_metric(fn: MetricFn) -> Metric:
    return Metric(fn=fn, selector=cs.numeric())


# ------------------------------------ FORMATTING ------------------------------------ #


def _percentage_string(
    fraction: pl.Expr, *, signed: bool = False, percent_sign: bool = True
) -> pl.Expr:
    """Format a fraction as a percentage string, optionally with an explicit sign."""
    pct = (fraction * 100).round(2)
    body = pl.format("{}%", pct) if percent_sign else pl.format("{}", pct)
    if signed:
        return (
            pl.when(pct > 0)
            .then(pl.format("+{}", body))
            .when(pct < 0)
            .then(body)
            .otherwise(pl.format("±{}", body))
        )
    return body


def _number_string(value: pl.Expr, *, signed: bool = False) -> pl.Expr:
    """Format a numeric value to four significant figures, optionally with a sign."""
    rounded = value.round_sig_figs(4)
    body = rounded.cast(pl.String)
    if signed:
        return (
            pl.when(rounded > 0)
            .then(pl.format("+{}", body))
            .when(rounded < 0)
            .then(body)
            .otherwise(pl.format("±{}", body))
        )
    return body


def _render_change(
    old: pl.Expr,
    new: pl.Expr,
    format_value: Callable[[pl.Expr, bool], pl.Expr],
) -> pl.Expr:
    """Render a change as ``<old> -> <new> (<delta>)``.

    ``format_value(value, signed)`` formats a value for display; ``old`` and ``new`` are
    rendered unsigned and the delta ``new - old`` is rendered signed (with an explicit
    ``+`` or ``-`` prefix).
    """
    return pl.format(
        "{} -> {} ({})",
        format_value(old, False),
        format_value(new, False),
        format_value(new - old, True),
    )


def _render_number_change(old: pl.Expr, new: pl.Expr) -> pl.Expr:
    """Render a numeric change as ``<old> -> <new> (<delta>)``."""
    return _render_change(
        old, new, lambda value, signed: _number_string(value, signed=signed)
    )


# -------------------------------------- METRICS ------------------------------------- #


def mean(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Change in mean, rendered as ``<mean(left)> -> <mean(right)> (<delta>)``."""
    return _render_number_change(left.mean(), right.mean())


def median(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Change in median, rendered as ``<median(left)> -> <median(right)> (<delta>)``."""
    return _render_number_change(left.median(), right.median())


def min(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Change in minimum, rendered as ``<min(left)> -> <min(right)> (<delta>)``."""
    return _render_number_change(left.min(), right.min())


def max(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Change in maximum, rendered as ``<max(left)> -> <max(right)> (<delta>)``."""
    return _render_number_change(left.max(), right.max())


def std(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Change in standard deviation, rendered as ``<std(left)> -> <std(right)>
    (<delta>)``."""
    return _render_number_change(left.std(), right.std())


def mean_absolute_deviation(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Mean of ``|right - left|``."""
    return (right - left).abs().mean()


def mean_relative_deviation(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Mean of ``|(right - left) / left|``. Yields ``inf`` or ``null`` where
    ``left`` is zero."""
    return ((right - left) / left).abs().mean()


def null_fraction_change(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Change in the fraction of null entries, rendered as ``<old> -> <new> (<delta>)``.

    ``old`` and ``new`` are the null percentages of the left and right side, and
    ``delta`` is their signed difference (``+`` when the right side has proportionally
    more nulls, ``-`` when it has fewer). Unlike the other presets, this applies to
    columns of any type.
    """
    return _render_change(
        left.is_null().mean(),
        right.is_null().mean(),
        lambda value, signed: _percentage_string(
            value, signed=signed, percent_sign=not signed
        ),
    )


def quantile(q: float) -> MetricFn:
    """Factory returning a metric for the change in the ``q``-quantile, rendered as
    ``<quantile(left)> -> <quantile(right)> (<delta>)``."""
    if not 0 <= q <= 1:
        raise ValueError(f"q must be in [0, 1], got {q}")

    def _quantile(left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return _render_number_change(left.quantile(q), right.quantile(q))

    return _quantile


DEFAULT_METRICS: dict[str, MetricFn | Metric] = {
    "Mean": mean,
    "Median": median,
    "Min": min,
    "Max": max,
    "Std": std,
    "Mean absolute deviation": mean_absolute_deviation,
    "Mean relative deviation": mean_relative_deviation,
    "Null%": Metric(fn=null_fraction_change, selector=cs.all()),
}
