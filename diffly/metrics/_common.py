# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from .change import DEFAULT_CHANGE_METRICS, ChangeMetric, ChangeMetricFn
from .data import DEFAULT_DATA_METRICS, DataMetric, DataMetricFn

Metric = ChangeMetric | DataMetric
"""A change or data metric paired with a column-applicability selector."""

MetricFn = ChangeMetricFn | DataMetricFn
"""A bare change or data metric callable, resolved to a :data:`Metric` by arity."""

DEFAULT_METRICS: dict[str, Metric] = {
    **DEFAULT_CHANGE_METRICS,
    **DEFAULT_DATA_METRICS,
}
"""All preset metrics, combining the change and data default sets."""
