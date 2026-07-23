# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause
"""Metrics describing the left and right datasets individually.

These characterize each side of a change so you can understand how the change affects
the data, rather than describing the change itself.
"""

from __future__ import annotations

import polars as pl
import polars.selectors as cs

from ._common import DataMetric


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
