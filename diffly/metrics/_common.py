# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import polars as pl
import polars.selectors as cs

ChangeMetricFn = Callable[[pl.Expr, pl.Expr], pl.Expr]
"""A change metric maps ``(left_expr, right_expr)`` to a scalar aggregation expression.

The expressions refer to the left-side and right-side values of a single column across
all joined rows.
"""

DataMetricFn = Callable[[pl.Expr], pl.Expr]
"""A data metric maps a single column expression to a scalar aggregation expression.

It is evaluated on the left and right side separately to describe each dataset on its
own, rather than the change between them.
"""

# Retained for backwards compatibility; a plain metric callable is a change metric.
MetricFn = ChangeMetricFn


@dataclass(frozen=True)
class ChangeMetric:
    """A metric quantifying the *change* between the two sides of a comparison.

    ``fn`` aggregates over ``right - left`` (e.g. the mean delta) to describe the change
    itself. Change metrics are rendered as extra columns in the "Columns" table,
    alongside the match rate.
    """

    fn: ChangeMetricFn
    selector: cs.Selector = field(default_factory=cs.numeric)


@dataclass(frozen=True)
class DataMetric:
    """A metric describing each dataset *individually*.

    ``fn`` is applied to the left and right side separately (e.g. the fraction of null
    entries on each side), characterizing the data rather than the change between the
    sides. Data metrics are rendered in a dedicated "Data Inspection" section, showing
    the left and right value side by side, followed by their signed delta for numeric
    values.

    ``formatter`` formats a single left/right value for display. ``delta_formatter``
    formats the (always non-negative) magnitude of the delta, which is rendered with an
    explicit sign; when ``None``, it falls back to ``formatter``. Both fall back to the
    default numeric precision when unset.
    """

    fn: DataMetricFn
    selector: cs.Selector = field(default_factory=cs.all)
    formatter: Callable[[Any], str] | None = None
    delta_formatter: Callable[[Any], str] | None = None


Metric = ChangeMetric | DataMetric
"""A change or data metric paired with a column-applicability selector."""
