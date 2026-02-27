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
            "name": ["tiger", "sloth", "elephant", "cat", "ant"],
            "weight_kg": [200.0, 5.0, None, 5.0, 0.00001],
            "speed_kph": [None, 0.27, 40.0, None, None],
            "life_expectancy": [11, 11, 48, 15, 1],
            "german_name": ["tiger", "faultieer", "elephant", "katze", "ameise"],
        }
    )

    right = pl.DataFrame(
        data={
            "name": ["tiger", "sloth", "elephant", "dolphin", "parrot", "cat"],
            "weight_kg": [200.0, 5.0, None, 50.0, 1.5, 5.0],
            "speed_kph": [None, 0.27, 40.0, 14.5, 56.0, None],
            "life_expectancy": [12, 12, 48, 35, 23, 15],
            "german_name": [
                "tiger",
                "faultier",
                "elefant",
                "delfin",
                "papagei",
                "katze",
            ],
        }
    )
    comp = compare_frames(left, right, primary_key=["name"])
    generate_summaries(comp)
