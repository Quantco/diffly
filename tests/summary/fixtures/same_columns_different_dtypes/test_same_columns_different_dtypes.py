# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from pathlib import Path

import polars as pl
import pytest

from diffly import compare_frames
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    left = pl.DataFrame(
        {
            "empty_col": pl.Series([], dtype=pl.String),
            "col_x": pl.Series([], dtype=pl.Int16),
            "col_y": pl.Series([], dtype=pl.Int8),
        }
    )
    right = pl.DataFrame(
        {
            "empty_col": pl.Series([], dtype=pl.String),
            "col_x": pl.Series([], dtype=pl.Int16),
            "col_y": pl.Series([], dtype=pl.Int32),
        }
    )
    comparison = compare_frames(left, right)

    generate_summaries(comparison, outdir=Path(__file__).parent / "gen")
