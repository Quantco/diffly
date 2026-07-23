# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause
"""Metrics computed per column when generating a summary.

Two families are provided:

- :class:`~diffly.metrics.change.ChangeMetric`s in :mod:`~diffly.metrics.change` describe the change between
  numeric columns itself by aggregating over ``right - left``.
- :class:`~diffly.metrics.data.DataMetric`s in :mod:`~diffly.metrics.data` describe the left and right
  datasets individually, explaining how a change affects the data.
"""

from __future__ import annotations

from . import change, data
from ._common import ALL_METRICS

__all__ = ["ALL_METRICS", "change", "data"]
