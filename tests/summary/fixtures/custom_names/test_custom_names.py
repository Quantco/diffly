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
            "id": [1, 2, 3, 4],
            "name": ["tiger", "sloth", "elephant", "ant"],
            "weight_kg": [200.0, 5.0, None, 0.00001],
            "speed_kph": [None, 0.27, 40.0, None],
            "life_expectancy": [11, 11, 48, 1],
            "habitat": ["jungle", "rainforest", "savanna", "everywhere"],
            "left_only_col": [
                "special_left_value",
                "another_left_value",
                "unique_left_data",
                "left_exclusive",
            ],
        }
    )

    right = pl.DataFrame(
        {
            "id": [1, 2, 3, 5, 6],
            "name": ["tiger", "sloth", "elephant", "dolphin", "parrot"],
            "weight_kg": [200.0, 5.0, None, 50.0, 1.5],
            "speed_kph": [None, 0.27, 40.0, 14.5, 56.0],
            "life_expectancy": [12, 12, 48, 35, 23],
            "habitat": ["jungle", "forest", "plains", "ocean", "tropics"],
            "right_only_col": [
                "special_right_value",
                "another_right_value",
                "unique_right_data",
                "new_right_data",
                "extra_right_info",
            ],
        }
    )

    comparison = compare_frames(left, right, primary_key=["id"])

    generate_summaries(
        comparison,
        outdir=Path(__file__).parent / "gen",
        left_name="Baseline Table",
        right_name="Feature Table",
    )
