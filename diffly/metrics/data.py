# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import polars as pl
import polars.selectors as cs

DataMetricFn = Callable[[pl.Expr], pl.Expr]
"""A data metric maps a single column expression to a scalar aggregation expression."""


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
    """Formats the magnitude of the delta, which is rendered with an explicit sign.

    Falls back to ``formatter`` when ``None``.
    """


# ----------------------------------- DATA METRICS ----------------------------------- #


def null_fraction(col: pl.Expr) -> pl.Expr:
    """Fraction of null entries in a column."""
    return col.is_null().mean()


DEFAULT_DATA_METRICS: dict[str, DataMetric] = {
    "Null%": DataMetric(
        fn=null_fraction,
        formatter=lambda value: f"{round(value * 100, 2)}%",
        delta_formatter=lambda value: f"{round(value * 100, 2)}",
    ),
    "Mean": DataMetric(
        fn=lambda col: col.mean(),
        formatter=lambda value: f"{round(value, 2)}",
        selector=cs.numeric(),
    ),
    "Median": DataMetric(
        fn=lambda col: col.median(),
        formatter=lambda value: f"{round(value, 2)}",
        selector=cs.numeric(),
    ),
    "Min": DataMetric(
        fn=lambda col: col.min(),
        formatter=lambda value: f"{round(value, 2)}",
        selector=cs.numeric(),
    ),
    "Max": DataMetric(
        fn=lambda col: col.max(),
        formatter=lambda value: f"{round(value, 2)}",
        selector=cs.numeric(),
    ),
}
"""Preset metrics describing the left and right datasets individually."""
