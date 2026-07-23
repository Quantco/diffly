# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import polars.selectors as cs
import pytest

from diffly import compare_frames
from diffly.metrics.data import DEFAULT_DATA_METRICS, DataMetric
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    # No primary key: row and column matches cannot be computed, but data metrics still
    # characterize each side individually and are shown in the Data Inspection section.
    left = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "price": [10.0, 20.0, None, 40.0, 50.0],
            "status": ["a", "b", "c", "d", "e"],
        }
    )
    right = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 6],
            "price": [10.0, 21.0, 30.0, 42.0, 50.0],
            "status": ["a", None, "x", None, "e"],
        }
    )
    comp = compare_frames(left, right)
    generate_summaries(
        comp,
        metrics={
            "Null%": DEFAULT_DATA_METRICS["Null%"],
            "Distinct": DataMetric(fn=lambda col: col.n_unique()),
            "Max": DataMetric(fn=lambda col: col.max(), selector=cs.string()),
        },
    )
