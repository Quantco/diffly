# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
from polars.testing import assert_frame_equal

from diffly import compare_frames


def test_left_only() -> None:
    left = pl.DataFrame({"id": ["a", "b", "c"], "x": [1, 2, 3]})
    right = pl.DataFrame({"id": ["a"], "x": [1]})
    primary_key = ["id"]

    expected = pl.DataFrame({"id": ["b", "c"], "x": [2, 3]})
    comparison = compare_frames(left, right, primary_key=primary_key)
    assert_frame_equal(expected, comparison.left_only())
    assert comparison.num_rows_left_only() == len(expected)


def test_right_only() -> None:
    left = pl.DataFrame({"id": ["a"], "x": [1]})
    right = pl.DataFrame({"id": ["a", "b", "c"], "x": [1, 2, 3]})
    primary_key = ["id"]

    expected = pl.DataFrame({"id": ["b", "c"], "x": [2, 3]})
    comparison = compare_frames(left, right, primary_key=primary_key)
    assert_frame_equal(expected, comparison.right_only())
    assert comparison.num_rows_right_only() == len(expected)


def test_left_only_lazy_parameter() -> None:
    left = pl.DataFrame({"id": ["a", "b"], "x": [1, 2]})
    right = pl.DataFrame({"id": ["a"], "x": [1]})
    primary_key = ["id"]
    comparison = compare_frames(left, right, primary_key=primary_key)

    result_eager = comparison.left_only()
    assert isinstance(result_eager, pl.DataFrame)

    result_lazy = comparison.left_only(lazy=True)
    assert isinstance(result_lazy, pl.LazyFrame)

    assert_frame_equal(result_eager, result_lazy.collect())


def test_right_only_lazy_parameter() -> None:
    left = pl.DataFrame({"id": ["a"], "x": [1]})
    right = pl.DataFrame({"id": ["a", "b"], "x": [1, 2]})
    primary_key = ["id"]
    comparison = compare_frames(left, right, primary_key=primary_key)

    result_eager = comparison.right_only()
    assert isinstance(result_eager, pl.DataFrame)

    result_lazy = comparison.right_only(lazy=True)
    assert isinstance(result_lazy, pl.LazyFrame)

    assert_frame_equal(result_eager, result_lazy.collect())
