# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from pathlib import Path

import polars as pl
import pytest

from diffly import compare_frames
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    num_rows = 100_000
    num_rows_add = 200_000
    left = pl.DataFrame({"key": range(num_rows), "value": [0] * num_rows})
    right = pl.DataFrame(
        {
            "key": range(1 + num_rows + num_rows_add),
            "value": [1] + [0] * num_rows + [1] * num_rows_add,
        }
    )
    comparison = compare_frames(left, right, primary_key=["key"])

    generate_summaries(comparison, outdir=Path(__file__).parent / "gen")
