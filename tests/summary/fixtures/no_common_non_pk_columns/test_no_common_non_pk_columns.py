# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import pytest

from diffly import compare_frames
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    left = pl.DataFrame(
        data={"name": ["tiger", "sloth", "elephant", "ant", "dolphin", "parrot"]}
    )
    right = pl.DataFrame(data={"name": ["tiger"]})
    comp = compare_frames(left, right, primary_key=["name"])
    generate_summaries(comp)
