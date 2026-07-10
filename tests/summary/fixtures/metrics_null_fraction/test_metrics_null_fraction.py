# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import polars.selectors as cs
import pytest

from diffly import compare_frames, metrics
from diffly.metrics import Metric
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    left = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "price": [10.0, 20.0, None, 40.0, 50.0],
            "status": ["a", "b", "c", "d", "e"],
        }
    )
    right = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "price": [10.0, 21.0, 30.0, 42.0, 50.0],
            "status": ["a", None, "x", None, "e"],
        }
    )
    comp = compare_frames(left, right, primary_key=["id"])
    generate_summaries(
        comp,
        metrics={
            # Numeric-only preset alongside a metric applied to all columns.
            "Mean": metrics.mean,
            "Null%": metrics.DEFAULT_METRICS["Null%"],
            # A user-supplied metric with a custom (string-only) selector.
            "str_len_delta": Metric(
                fn=lambda left, right: (
                    right.str.len_chars() - left.str.len_chars()
                ).mean(),
                selector=cs.string(),
            ),
        },
    )
