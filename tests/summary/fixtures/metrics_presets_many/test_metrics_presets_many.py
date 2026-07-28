# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import pytest

from diffly import compare_frames, metrics
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    left = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "price": [10.0, 20.0, 30.0, 40.0, 50.0],
            "qty": [1, 2, 3, 4, 5],
            "status": ["a", "b", "c", "d", "e"],
        }
    )
    right = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "price": [10.0, 21.0, 30.0, 42.0, 50.0],
            "qty": [1, 2, 3, 5, 5],
            "status": ["a", "b", "x", "d", "e"],
        }
    )
    comp = compare_frames(left, right, primary_key=["id"])
    generate_summaries(
        comp,
        change_metrics={
            "Mean diff": metrics.change.mean,
            "Median diff": metrics.change.median,
            "Min diff": metrics.change.min,
            "Max diff": metrics.change.max,
            "Std diff": metrics.change.std,
            "Mean absolute diff": metrics.change.mean_absolute_deviation,
            "Mean relative diff: metrics.change.mean_relative_deviation,
        },
    )
