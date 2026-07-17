# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

import polars as pl
import polars.selectors as cs


@dataclass(frozen=True)
class Metric:
    """A metric function paired with a column-applicability selector.

    ``kind`` selects the summary section the metric is rendered in: ``"change"`` metrics
    appear as columns in the "Columns" table, while ``"data"`` metrics get their own
    "Data Inspection" section.
    """

    fn: MetricFn
    selector: cs.Selector
    kind: Literal["change", "data"] = "change"


MetricFn = Callable[[pl.Expr, pl.Expr], pl.Expr]
"""A metric function maps ``(left_expr, right_expr)`` to a scalar aggregation
expression.

The expressions refer to the left-side and right-side values of a single column across
all joined rows.
"""
