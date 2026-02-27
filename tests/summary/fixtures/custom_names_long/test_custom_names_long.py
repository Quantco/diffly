# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from pathlib import Path

import polars as pl
import pytest

from diffly import compare_frames
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    N: int = 100
    left = pl.DataFrame(
        {
            "id": range(4 * N),
            "name": ["tiger", "sloth", "elephant", "ant"] * N,
        }
    )

    right = pl.DataFrame(
        {
            "id": range(5 * N),
            "name": ["tiger", "elephant", "sloth", "dolphin", "parrot"] * N,
        }
    )

    comparison = compare_frames(left, right, primary_key=["id"])

    generate_summaries(
        comparison,
        outdir=Path(__file__).parent / "gen",
        left_name="Left table with a very long name that exceeds the usual length",
        right_name="Right table with a very long name that exceeds the usual length",
    )
