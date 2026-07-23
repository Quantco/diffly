# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from .change import ChangeMetric
from .data import DataMetric

Metric = ChangeMetric | DataMetric
"""A change or data metric paired with a column-applicability selector."""
