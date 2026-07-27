# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import polars.selectors as cs
import pytest

from diffly import compare_frames, metrics
from diffly.metrics.change import ChangeMetric
from diffly.metrics.data import DEFAULT_DATA_METRICS, DataMetric
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    left = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "price": [10.0, 20.0, None, 40.0, 50.0],
            # `score` is fully populated on the left but entirely null on the right, so a
            # numeric data metric yields a ``float -> None`` pair.
            "score": [1.0, 2.0, 3.0, 4.0, 5.0],
            "status": ["a", "b", "c", "d", "e"],
        }
    )
    right = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "price": [10.0, 21.0, 30.0, 42.0, 50.0],
            "score": pl.Series([None] * 5, dtype=pl.Float64),
            "status": ["a", None, "x", None, "e"],
        }
    )
    comp = compare_frames(left, right, primary_key=["id"])
    generate_summaries(
        comp,
        metrics={
            # Numeric-only preset alongside a metric applied to all columns.
            "Mean": metrics.change.mean,
            # A change metric returning a (non-numeric) string value.
            "Trend": ChangeMetric(
                fn=lambda left, right: (
                    pl.when((right - left).mean() >= 0)
                    .then(pl.lit("up"))
                    .otherwise(pl.lit("down"))
                ),
            ),
            "Null%": DEFAULT_DATA_METRICS["Null%"],
            # A second, numeric data metric to render more than one data column.
            "Distinct": DataMetric(fn=lambda col: col.n_unique()),
            # A numeric data metric without a custom formatter, so floats fall back to the
            # default precision and a null side renders as ``None``.
            "Avg": DataMetric(fn=lambda col: col.mean(), selector=cs.numeric()),
            # A non-numeric data metric: rendered as ``left -> right`` without a delta.
            "Max": DataMetric(fn=lambda col: col.max(), selector=cs.string()),
            # A user-supplied change metric with a custom (string-only) selector.
            "str_len_delta": ChangeMetric(
                fn=lambda left, right: (
                    right.str.len_chars() - left.str.len_chars()
                ).mean(),
                selector=cs.string(),
            ),
        },
    )
