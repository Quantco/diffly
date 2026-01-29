# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import random

import polars as pl
import pytest
from pytest_lazy_fixtures import lf

from diffly import compare_frames


def _shuffle_df(df: pl.DataFrame) -> pl.DataFrame:
    column_names = df.columns
    random.shuffle(column_names)
    return df.select(column_names).sample(fraction=1)


@pytest.fixture()
def df_full() -> pl.DataFrame:
    return pl.DataFrame({"id": ["a", "b", "c"], "x": [1, 2, 3], "y": [4.0, 5.0, 6.0]})


@pytest.fixture()
def df_subset_rows(df_full: pl.DataFrame) -> pl.DataFrame:
    return df_full.slice(0, 2)


@pytest.fixture()
def df_subset_cols(df_full: pl.DataFrame) -> pl.DataFrame:
    return df_full.select(["id", "x"])


@pytest.mark.parametrize(
    ("left", "right", "result"),
    [
        (lf("df_full"), lf("df_full"), True),
        (lf("df_full"), lf("df_subset_cols"), False),
        (lf("df_full"), lf("df_subset_rows"), False),
    ],
)
@pytest.mark.parametrize("primary_key", [["id"], None])
@pytest.mark.parametrize("shuffle", [False, True])
def test_equal(
    left: pl.DataFrame,
    right: pl.DataFrame,
    result: bool,
    primary_key: list[str] | None,
    shuffle: bool,
) -> None:
    left = _shuffle_df(left) if shuffle else left
    right = _shuffle_df(right) if shuffle else right
    assert compare_frames(left, right, primary_key=primary_key).equal() == result


def test_equal_nested_dtypes() -> None:
    left = pl.DataFrame(
        {
            "c": [True, False],
            "a": [1, 2],
            "b": [["foo"], ["bar"]],
            "d": [[1.0], [2.0]],
            "g": pl.Series([[1.0], [2.0]], dtype=pl.Array(pl.Float32, 1)),
            "e": [{"k": "v1"}, {"k": "v2"}],
            "f": [[{"k": "v1"}], [{"k": "v2"}]],
        }
    )
    right = pl.DataFrame(
        {
            "d": [[2.0], [1.0]],
            "f": [[{"k": "v2"}], [{"k": "v1"}]],
            "e": [{"k": "v2"}, {"k": "v1"}],
            "b": [["bar"], ["foo"]],
            "a": [2, 1],
            "g": pl.Series([[2.0], [1.0]], dtype=pl.Array(pl.Float32, 1)),
            "c": [False, True],
        }
    )
    assert compare_frames(left, right).equal()


def test_equal_unequal_schemas() -> None:
    left = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
    right = pl.DataFrame({"a": [1, 2], "c": [3, 4]})
    assert not compare_frames(left, right).equal()


def test_equal_with_nulls() -> None:
    df1 = pl.DataFrame({"a": [None, 2], "b": [1, 2]})
    df2 = pl.DataFrame({"a": [None, 2], "b": [1, 2]})
    assert compare_frames(df1, df2).equal()
    assert compare_frames(df1, df2, primary_key=["a"]).equal()


def test_equal_empty() -> None:
    left = pl.DataFrame({})
    right = pl.DataFrame({})
    assert compare_frames(left, right).equal()


def test_equal_one_empty() -> None:
    left = pl.DataFrame({"a": [1]})
    right = pl.DataFrame({})
    assert not compare_frames(left, right).equal()


def test_equal_without_primary_key() -> None:
    left = pl.DataFrame({"a": [1, 2], "b": [5.0, 7.0]})
    right = pl.DataFrame({"a": [2, 1], "b": [7.0, 5.1]})
    assert compare_frames(left, right, abs_tol=0.5).equal()


def test_equal_ignore_dtypes() -> None:
    left = pl.DataFrame({"a": [1, 2]}, schema={"a": pl.Int64})
    right = pl.DataFrame({"a": [1, 2]}, schema={"a": pl.Int32})
    comparison = compare_frames(left, right)
    assert not comparison.equal()
    assert comparison.equal(check_dtypes=False)
