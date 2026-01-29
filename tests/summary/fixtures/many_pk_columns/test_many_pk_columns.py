# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import pytest

from diffly import compare_frames
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    base_data = {
        "name": ["tiger", "sloth", "elephant", "ant", "dolphin" * 4, "parrot"],
        "weight_kg": [200.0, 5.0, None, 0.00001, 50.0, 1.5],
        "speed_kph": [None, 0.27, 40.0, None, 14.5, 56.0],
        "life_expectancy": [12, 12, 48, 1, 35, 23],
    }
    left = pl.DataFrame(
        data={f"{k}_{i}": v for i in range(20) for k, v in base_data.items()},
    )
    right = left.head(4)
    comp = compare_frames(left, right, primary_key=[f"name_{i}" for i in range(20)])
    generate_summaries(comp)
