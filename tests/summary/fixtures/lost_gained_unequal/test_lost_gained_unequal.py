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
            "name": ["tiger", "sloth", "elephant", "ant"],
            "weight_kg": [200.0, 5.0, None, 0.00001],
            "speed_kph": [None, 0.27, 40.0, None],
            "life_expectancy": [11, 11, 48, 1],
            "german_name": ["tiger", "faultieer", "elephant", "ameise"],
        }
    )

    right = pl.DataFrame(
        data={
            "name": ["tiger", "sloth", "elephant", "dolphin", "parrot"],
            "weight_kg": [200.0, 5.0, None, 50.0, 1.5],
            "speed_kph": [None, 0.27, 40.0, 14.5, 56.0],
            "life_expectancy": [12, 12, 48, 35, 23],
            "german_name": ["tiger", "faultier", "elefant", "delfin", "papagei"],
        }
    )
    comp = compare_frames(left, right, primary_key=["name"])
    generate_summaries(comp)
