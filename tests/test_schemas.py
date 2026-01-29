# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import pytest
from pytest_lazy_fixtures import lf

from diffly import compare_frames


@pytest.fixture()
def df_full() -> pl.DataFrame:
    return pl.DataFrame(
        {"id": ["a", "b", "c"], "x": [1, 2, 3], "y": [4.0, 5.0, 6.0], "z": [7, 8, 9]}
    )


@pytest.fixture()
def df_subset_cols(df_full: pl.DataFrame) -> pl.DataFrame:
    return df_full.select(["id", "x"])


@pytest.fixture()
def df_subset_rows(df_full: pl.DataFrame) -> pl.DataFrame:
    return df_full.slice(0, 2)


@pytest.fixture()
def df_subset_cols_f32(df_full: pl.DataFrame) -> pl.DataFrame:
    return df_full.select("id", pl.col("x").cast(pl.Float32))


def test_left_right() -> None:
    left = pl.DataFrame({"id": ["a", "b"], "x": [1, 2], "y": [3.0, 4.0], "z": [5, 6]})
    right = pl.DataFrame({"id": ["a", "b"], "x": [1, 2]})
    comparison = compare_frames(left, right)
    assert comparison.schemas.left() == {
        "id": pl.String,
        "x": pl.Int64,
        "y": pl.Float64,
        "z": pl.Int64,
    }
    assert comparison.schemas.right() == {
        "id": pl.String,
        "x": pl.Int64,
    }


def test_column_names() -> None:
    left = pl.DataFrame({"id": ["a", "b"], "x": [1, 2], "y": [3.0, 4.0], "z": [5, 6]})
    right = pl.DataFrame({"id": ["a", "b"], "x": [1, 2]})
    comparison = compare_frames(left, right)
    assert comparison.schemas.left().column_names() == {"id", "x", "y", "z"}
    assert comparison.schemas.right().column_names() == {"id", "x"}


@pytest.mark.parametrize(
    ("left", "right", "check_dtypes", "result"),
    [
        (lf("df_full"), lf("df_subset_rows"), False, True),
        (lf("df_full"), lf("df_subset_rows"), True, True),
        (lf("df_full"), lf("df_subset_cols"), False, False),
        (lf("df_full"), lf("df_subset_cols"), True, False),
        (lf("df_subset_cols"), lf("df_subset_cols_f32"), False, True),
        (lf("df_subset_cols"), lf("df_subset_cols_f32"), True, False),
    ],
)
def test_equal(
    left: pl.DataFrame, right: pl.DataFrame, check_dtypes: bool, result: bool
) -> None:
    assert (
        compare_frames(left, right).schemas.equal(check_dtypes=check_dtypes) == result
    )


def test_in_common() -> None:
    left = pl.DataFrame({"id": ["a"], "x": [1], "y": [2.0]})
    right = pl.DataFrame({"id": ["a"], "x": [1.0]})
    comparison = compare_frames(left, right)
    assert comparison.schemas.in_common() == {
        "id": (pl.String, pl.String),
        "x": (pl.Int64, pl.Float64),
    }


def test_in_common_matching_dtypes() -> None:
    left = pl.DataFrame({"id": ["a"], "x": [1], "y": [2.0]})
    right = pl.DataFrame({"id": ["a"], "x": [1.0]})
    comparison = compare_frames(left, right)
    assert comparison.schemas.in_common().matching_dtypes() == {"id": pl.String}


def test_in_common_mismatching_dtypes() -> None:
    left = pl.DataFrame({"id": ["a"], "x": [1], "y": [2.0]})
    right = pl.DataFrame({"id": ["a"], "x": [1.0]})
    comparison = compare_frames(left, right)
    assert comparison.schemas.in_common().mismatching_dtypes() == {
        "x": (pl.Int64, pl.Float64)
    }


def test_in_common_column_names() -> None:
    left = pl.DataFrame({"id": ["a"], "x": [1], "y": [2.0]})
    right = pl.DataFrame({"id": ["a"], "x": [1.0]})
    comparison = compare_frames(left, right)
    assert comparison.schemas.in_common().column_names() == {"id", "x"}


def test_only() -> None:
    left = pl.DataFrame({"id": ["a"], "x": [1], "y": [2.0], "z": [3]})
    right = pl.DataFrame({"id": ["a"], "x": [1]})
    comparison = compare_frames(left, right)
    assert comparison.schemas.left_only().column_names() == {"y", "z"}
    assert comparison.schemas.left_only() == {
        "y": pl.Float64,
        "z": pl.Int64,
    }
    assert comparison.schemas.right_only().column_names() == set()
    assert comparison.schemas.right_only() == dict()
