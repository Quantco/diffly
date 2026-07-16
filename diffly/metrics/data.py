# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

"""Metrics describing the left and right datasets individually.

These characterize each side of a change so you can understand how the change affects
the data, rather than describing the change itself.
"""

from __future__ import annotations

from collections.abc import Callable

import polars as pl
import polars.selectors as cs

from ._common import Metric, MetricFn


def null_fraction_change(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Change in the fraction of null entries, rendered as ``<old> -> <new> (<delta>)``.

    ``old`` and ``new`` are the null percentages of the left and right side, and
    ``delta`` is their signed difference (``+`` when the right side has proportionally
    more nulls, ``-`` when it has fewer). This metric function can be applied to columns
    of any type.
    """
    return _render_change(
        left.is_null().mean(),
        right.is_null().mean(),
        lambda value, signed: _percentage_string(
            value, signed=signed, percent_sign=not signed
        ),
    )


def mean_change(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Change in the mean, rendered as ``<old mean> -> <new mean> (<delta>)``."""
    return _render_numeric_change(left.mean(), right.mean())


def median_change(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Change in the median, rendered as ``<old median> -> <new median> (<delta>)``."""
    return _render_numeric_change(left.median(), right.median())


def min_change(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Change in the minimum, rendered as ``<old min> -> <new min> (<delta>)``."""
    return _render_numeric_change(left.min(), right.min())


def max_change(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Change in the maximum, rendered as ``<old max> -> <new max> (<delta>)``."""
    return _render_numeric_change(left.max(), right.max())


def std_change(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Change in the standard deviation, rendered as ``<old std> -> <new std>
    (<delta>)``."""
    return _render_numeric_change(left.std(), right.std())


DEFAULT_DATA_METRICS: dict[str, MetricFn | Metric] = {
    "Null% (data)": Metric(fn=null_fraction_change, selector=cs.all()),
    "Mean (data)": Metric(fn=mean_change, selector=cs.numeric()),
    "Median (data)": Metric(fn=median_change, selector=cs.numeric()),
    "Min (data)": Metric(fn=min_change, selector=cs.numeric()),
    "Max (data)": Metric(fn=max_change, selector=cs.numeric()),
    "Std (data)": Metric(fn=std_change, selector=cs.numeric()),
}
"""Preset metrics describing the left and right datasets individually."""


# ------------------------------------------------------------------------------------ #
#                                    UTILITY METHODS                                   #
# ------------------------------------------------------------------------------------ #


def _percentage_string(
    fraction: pl.Expr, *, signed: bool = False, percent_sign: bool = True
) -> pl.Expr:
    """Format a fraction as a percentage string, optionally with an explicit sign."""
    pct = (fraction * 100).round(2)
    body = pl.format("{}%", pct) if percent_sign else pl.format("{}", pct)
    if signed:
        return pl.when(pct >= 0).then(pl.format("+{}", body)).otherwise(body)
    return body


def _numeric_string(value: pl.Expr, signed: bool) -> pl.Expr:
    """Format a numeric value for display, optionally with an explicit sign."""
    rounded = value.round_sig_figs(4)
    body = pl.format("{}", rounded)
    if signed:
        return pl.when(rounded >= 0).then(pl.format("+{}", body)).otherwise(body)
    return body


def _render_numeric_change(old: pl.Expr, new: pl.Expr) -> pl.Expr:
    """Render a change between two numeric aggregations as ``<old> -> <new>
    (<delta>)``."""
    return _render_change(old, new, _numeric_string)


def _render_change(
    old: pl.Expr,
    new: pl.Expr,
    format_value: Callable[[pl.Expr, bool], pl.Expr],
) -> pl.Expr:
    """Render a change as ``<old> -> <new> (<delta>)``.

    ``format_value(value, signed)`` formats a value for display; ``old`` and ``new`` are
    rendered unsigned and the delta ``new - old`` is rendered signed (with an explicit
    ``+`` prefix for positive values).
    """
    return pl.format(
        "{} -> {} ({})",
        format_value(old, False),
        format_value(new, False),
        format_value(new - old, True),
    )
