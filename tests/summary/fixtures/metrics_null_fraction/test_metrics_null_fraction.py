# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import polars.selectors as cs
import pytest

from diffly import compare_frames
from diffly.metrics import Metric
from diffly.metrics.data import DEFAULT_DATA_METRICS
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
            "Null% (data)": DEFAULT_DATA_METRICS["Null% (data)"],
            "Mean (data)": DEFAULT_DATA_METRICS["Mean (data)"],
            "Median (data)": DEFAULT_DATA_METRICS["Median (data)"],
            "Min (data)": DEFAULT_DATA_METRICS["Min (data)"],
            "Max (data)": DEFAULT_DATA_METRICS["Max (data)"],
            "Std (data)": DEFAULT_DATA_METRICS["Std (data)"],
            # A user-supplied metric with a custom (string-only) selector.
            "str_len_delta": Metric(
                fn=lambda left, right: (
                    right.str.len_chars() - left.str.len_chars()
                ).mean(),
                selector=cs.string(),
            ),
        },
    )
