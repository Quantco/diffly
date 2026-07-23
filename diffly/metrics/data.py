# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause
"""Metrics describing the left and right datasets individually.

These characterize each side of a change so you can understand how the change affects
the data, rather than describing the change itself.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import polars as pl
import polars.selectors as cs

DataMetricFn = Callable[[pl.Expr], pl.Expr]
"""A data metric maps a single column expression to a scalar aggregation expression.

It is evaluated on both data frames individually, and the change between them is
rendered.
"""


@dataclass(frozen=True)
class DataMetric:
    """A metric describing each dataset *individually*.

    Data metrics are rendered in a dedicated "Data Inspection" section, showing the left
    and right value side by side, followed by their signed delta for numeric values.
    """

    fn: DataMetricFn
    """Applied to the left and right side separately, characterizing the data rather
    than the change between the sides."""

    selector: cs.Selector = field(default_factory=cs.all)
    """Selects the columns the metric applies to; defaults to all columns."""

    formatter: Callable[[Any], str] | None = None
    """Formats a single left/right value for display.

    Falls back to the default numeric precision when unset.
    """

    delta_formatter: Callable[[Any], str] | None = None
    """Formats the (always non-negative) magnitude of the delta, which is rendered with
    an explicit sign.

    Falls back to ``formatter`` when ``None``, which in turn falls back to the default
    numeric precision when unset.
    """


def null_fraction(col: pl.Expr) -> pl.Expr:
    """Fraction of null entries in a column."""
    return col.is_null().mean()


DEFAULT_DATA_METRICS: dict[str, DataMetric] = {
    "Null%": DataMetric(
        fn=null_fraction,
        selector=cs.all(),
        formatter=lambda value: f"{round(value * 100, 2)}%",
        delta_formatter=lambda value: f"{round(value * 100, 2)}",
    ),
}
"""Preset metrics describing the left and right datasets individually."""
