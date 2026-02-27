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
            "name": ["tiger", "lion", "bear", "elephant"],
            "weight_kg": [200, 150, 100, None],
        },
        schema={
            "name": pl.String,
            "weight_kg": pl.Float64,
        },
    )
    right = pl.DataFrame(
        {
            "name": ["tiger", "lion", "bear", "elephant"],
            "weight_kg": [[200], [150, 170], [], None],
        },
        schema={
            "name": pl.String,
            "weight_kg": pl.List(pl.Float64),
        },
    )
    comparison = compare_frames(left, right, primary_key=["name"])

    generate_summaries(comparison, outdir=Path(__file__).parent / "gen")
