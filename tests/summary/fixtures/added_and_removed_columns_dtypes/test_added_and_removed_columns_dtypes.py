# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import pytest

from diffly import compare_frames
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    left = pl.DataFrame(
        schema={
            "empty_col": pl.String,
            "col_a": pl.Int64,
            "col_b": pl.Int32,
            "col_x": pl.Int16,
            "col_y": pl.Int8,
        }
    )
    right = pl.DataFrame(
        schema={
            "empty_col": pl.String,
            "col_d": pl.Int64,
            "col_x": pl.UInt16,
            "col_y": pl.Float32,
        }
    )
    comp = compare_frames(left, right)
    generate_summaries(comp)
