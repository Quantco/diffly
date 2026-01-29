# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from diffly import compare_frames


def test_joined() -> None:
    left = pl.DataFrame({"id": ["a", "b", "c"], "x": [1, 2, 3], "y": [4.0, 5.0, 6.0]})
    right = pl.DataFrame({"id": ["a", "b", "c"], "x": [1, 2, 3], "y": [4.0, 5.0, 6.0]})
    primary_key = ["id"]

    expected = pl.DataFrame(
        {
            "id": ["a", "b", "c"],
            "x_left": [1, 2, 3],
            "x_right": [1, 2, 3],
            "y_left": [4.0, 5.0, 6.0],
            "y_right": [4.0, 5.0, 6.0],
        }
    )
    comparison = compare_frames(left, right, primary_key=primary_key)
    assert_frame_equal(expected, comparison.joined())
    assert comparison.num_rows_joined() == len(expected)


def test_joined_missing_primary_key() -> None:
    left = pl.DataFrame({"id": ["a", "b"], "value": [1, 2]})
    right = pl.DataFrame({"id": ["a"], "value": [1]})
    comparison = compare_frames(left, right)
    with pytest.raises(ValueError):
        _ = comparison.joined()


def test_joined_lazy_parameter() -> None:
    left = pl.DataFrame({"id": ["a", "b"], "value": [1, 2]})
    right = pl.DataFrame({"id": ["a", "b"], "value": [1, 2]})
    primary_key = ["id"]
    comparison = compare_frames(left, right, primary_key=primary_key)

    result_eager = comparison.joined()
    assert isinstance(result_eager, pl.DataFrame)

    result_lazy = comparison.joined(lazy=True)
    assert isinstance(result_lazy, pl.LazyFrame)

    assert_frame_equal(result_eager, result_lazy.collect())
