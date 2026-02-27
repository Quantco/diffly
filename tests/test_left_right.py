# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl

from diffly import compare_frames


def test_row_counts() -> None:
    left = pl.DataFrame({"id": [1, 2, 3, 4, 5, 6, 7, 8, 9], "value": range(9)})
    right = pl.DataFrame({"id": [1, 2, 3], "value": range(3)})
    comparison = compare_frames(left, right)
    assert comparison.num_rows_left() == 9
    assert comparison.num_rows_right() == 3
