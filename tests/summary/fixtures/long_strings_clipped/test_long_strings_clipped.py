# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import json

import polars as pl
import pytest

from diffly import compare_frames
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    obj_0 = [{"id": k, "title": f"Document {k}"} for k in range(100)]
    obj_1 = [{"id": k + 1, "title": f"Document {k + 1}"} for k in range(100)]
    long_str_0 = json.dumps(obj_0)
    long_str_1 = json.dumps(obj_1)

    left = pl.DataFrame(
        {
            "key": [1, 2],
            "value": [long_str_0] * 2,
        }
    )
    right = pl.DataFrame(
        {
            "key": [1, 2],
            "value": [long_str_1, long_str_0],
        }
    )
    comparison = compare_frames(left, right, primary_key=["key"])

    generate_summaries(comparison)
