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


def _make_data_metric(fn: MetricFn, selector: cs.Selector = cs.all()) -> Metric:
    """Wrap a metric function as a data metric, applicable to all columns by default."""
    return Metric(fn=fn, selector=selector, kind="data")


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


DEFAULT_DATA_METRICS: dict[str, MetricFn | Metric] = {
    "Null%": _make_data_metric(null_fraction_change),
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
