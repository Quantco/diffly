# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import polars as pl
import polars.selectors as cs

ChangeMetricFn = Callable[[pl.Expr, pl.Expr], pl.Expr]
"""A change metric maps the difference between two joined columns from the compared data
frames to a scalar aggregation expression."""

DataMetricFn = Callable[[pl.Expr], pl.Expr]
"""A data metric maps a single column expression to a scalar aggregation expression.

It is evaluated on both data frames individually, and the change between them is
rendered.
"""

# Retained for backwards compatibility; a plain metric callable is a change metric.
MetricFn = ChangeMetricFn


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


Metric = ChangeMetric | DataMetric
"""A change or data metric paired with a column-applicability selector."""
