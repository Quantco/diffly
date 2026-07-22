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

    ``kind`` distinguishes two semantically different metric families:

    - ``"change"`` metrics aggregate over ``right - left`` to quantify the *change*
      between the two sides (e.g. the mean delta). They describe the change itself and
      are rendered as extra columns in the "Columns" table, alongside the match rate.
    - ``"data"`` metrics describe each dataset *individually* (e.g. the fraction of null
      entries on each side), characterizing the data rather than the change between the
      sides. They are rendered in a dedicated "Data Inspection" section.
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
