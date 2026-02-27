# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import pytest

from diffly import compare_frames
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    left = pl.DataFrame(
        data={
            "car": ["animal", "car", "plane", "plane"],
            "subcategory": ["mammal", "sedan", "jet", "propeller"],
            "weight": [1.0, 2.0, 3.0, 4.0],
        }
    )
    right = pl.DataFrame(
        data={
            "car": ["animal", "car", "plane"],
            "subcategory": ["mammal", "sedan", "jet"],
            "weight": [1.0, 2.0, 4.0],
        }
    )
    comp = compare_frames(left, right, primary_key=["car", "subcategory"])
    generate_summaries(comp)
