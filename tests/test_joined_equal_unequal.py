# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import datetime as dt
import itertools
from collections import defaultdict
from collections.abc import Callable, Mapping
from itertools import combinations
from typing import Literal

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from diffly import compare_frames


def test_joined_equal() -> None:
    left = pl.DataFrame(
        {
            "id": ["a", "b", "c"],
            "x": [1, 2, 3],
            "y": [4.0, 5.0, 6.0],
            "z": [7, 8, 9],
        }
    )
    right = pl.DataFrame(
        {
            "id": ["a", "b", "c"],
            "x": [1, 2, 99],
            "y": [4.0, 99.0, 6.0],
            "z": [7, 8, 9],
        }
    )
    primary_key = ["id"]
    schema = {
        "id": pl.String,
        "x_left": pl.Int64,
        "x_right": pl.Int64,
        "y_left": pl.Float64,
        "y_right": pl.Float64,
        "z_left": pl.Int64,
        "z_right": pl.Int64,
    }
    expected_column_to_equal_rows = {
        "x": pl.DataFrame(
            [
                ["a", 1, 1, 4.0, 4.0, 7, 7],
                ["b", 2, 2, 5.0, 99.0, 8, 8],
            ],
            schema=schema,
            orient="row",
        ),
        "y": pl.DataFrame(
            [
                ["a", 1, 1, 4.0, 4.0, 7, 7],
                ["c", 3, 99, 6.0, 6.0, 9, 9],
            ],
            schema=schema,
            orient="row",
        ),
        "z": pl.DataFrame(
            [
                ["a", 1, 1, 4.0, 4.0, 7, 7],
                ["b", 2, 2, 5.0, 99.0, 8, 8],
                ["c", 3, 99, 6.0, 6.0, 9, 9],
            ],
            schema=schema,
            orient="row",
        ),
    }

    comparison = compare_frames(left, right, primary_key=primary_key)

    # Check that `joined_equal` works correctly for all combinations of columns
    common_columns = ["x", "y", "z"]
    for i in range(0, len(common_columns) + 1):
        for subset in combinations(common_columns, i):
            actual = comparison.joined_equal(*subset)
            num_actual = comparison.num_rows_joined_equal(*subset)

            if not subset:
                # subset is empty, so we must have compared all common columns
                subset = tuple(common_columns)

            # Build expected DataFrame for the entire subset by semi-joining the
            # expected DataFrames for individual columns
            expected_subset = expected_column_to_equal_rows[subset[0]]
            for column in subset[1:]:
                expected_subset = expected_subset.join(
                    expected_column_to_equal_rows[column], on=["id"], how="semi"
                )

            assert_frame_equal(expected_subset, actual)
            assert len(expected_subset) == num_actual


def test_joined_equal_all_keys() -> None:
    df1 = pl.DataFrame({"a": [1, 2], "b": [True, False]})
    df2 = pl.DataFrame({"a": [2, 1], "b": [False, True]})
    comparison = compare_frames(df1, df2, primary_key=["a", "b"])
    assert_frame_equal(
        df1,
        comparison.joined_equal(),
        check_row_order=False,
        check_column_order=False,
    )


@pytest.mark.parametrize(
    "select",
    [
        "all",
        "subset",
        ["x"],
        ["x", "y"],
    ],
)
def test_joined_unequal(
    select: Literal["all", "subset"] | list[str],
) -> None:
    left = pl.DataFrame({"id": ["a", "b", "c"], "x": [1, 2, 3], "y": [4.0, 5.0, 6.0]})
    right = pl.DataFrame(
        {"id": ["a", "b", "c"], "x": [1, 99, 3], "y": [99.0, 5.0, 6.0]}
    )
    primary_key = ["id"]
    schema = {
        "id": pl.String,
        "x_left": pl.Int64,
        "x_right": pl.Int64,
        "y_left": pl.Float64,
        "y_right": pl.Float64,
    }
    expected_column_to_unequal_rows = {
        "x": pl.DataFrame(
            [
                ["b", 2, 99, 5.0, 5.0],
            ],
            schema=schema,
            orient="row",
        ),
        "y": pl.DataFrame(
            [
                ["a", 1, 1, 4.0, 99.0],
            ],
            schema=schema,
            orient="row",
        ),
    }
    comparison = compare_frames(left, right, primary_key=primary_key)

    # Check that `joined_unequal` works correctly for all combinations of columns
    all_combinations = [[], ["x"], ["y"], ["x", "y"]]
    for subset in all_combinations:
        actual = comparison.joined_unequal(*subset, select=select)
        num_actual = comparison.num_rows_joined_unequal(*subset)

        if not subset:
            # subset is empty, so we must have compared all common columns
            subset = ["x", "y"]
        expected_subset = pl.concat(
            [expected_column_to_unequal_rows[col] for col in subset]
        ).unique()
        columns_to_select = None
        if select == "subset":
            columns_to_select = list(subset)
        elif isinstance(select, list):
            columns_to_select = list(subset) + select
        if columns_to_select is not None:
            expected_subset = expected_subset.select(
                *primary_key,
                *itertools.chain.from_iterable(
                    [f"{col}_left", f"{col}_right"] for col in set(columns_to_select)
                ),
            )
        assert_frame_equal(expected_subset, actual, check_row_order=False)
        assert len(expected_subset) == num_actual


@pytest.mark.parametrize(
    "select",
    [
        "all",
        "subset",
        ["x"],
        ["x", "y"],
    ],
)
def test_joined_unequal_uncommon_column(
    select: Literal["all", "subset"] | list[str],
) -> None:
    left = pl.DataFrame({"id": ["a"], "x": [1], "y": [2.0]})
    right = pl.DataFrame({"id": ["a"], "x": [1], "y": [2.0], "z": [3]})
    primary_key = ["id"]
    comparison = compare_frames(left, right, primary_key=primary_key)
    with pytest.raises(ValueError, match="are not common columns."):
        comparison.joined_unequal("nonexistent_column", select=select)


def test_joined_unequal_uncommon_select_column() -> None:
    left = pl.DataFrame({"id": ["a"], "x": [1], "y": [2.0]})
    right = pl.DataFrame({"id": ["a"], "x": [1], "y": [2.0], "z": [3]})
    primary_key = ["id"]
    comparison = compare_frames(left, right, primary_key=primary_key)
    with pytest.raises(ValueError, match="are not common columns."):
        comparison.joined_unequal("id", select=["nonexistent_column"])


@pytest.mark.parametrize(
    "select",
    [
        "all",
        "subset",
        ["x"],
        ["x", "y"],
    ],
)
def test_joined_unequal_primary_key_column(
    select: Literal["all", "subset"] | list[str],
) -> None:
    left = pl.DataFrame({"id": ["a"], "x": [1]})
    right = pl.DataFrame({"id": ["a"], "x": [2]})
    primary_key = "id"
    comparison = compare_frames(left, right, primary_key=primary_key)
    with pytest.raises(ValueError, match="are not common columns."):
        comparison.joined_unequal(primary_key, select=select)


@pytest.mark.parametrize(
    "select",
    [
        "not-all",
        "not-subset",
        {"x", "y"},
    ],
)
def test_joined_unequal_invalid_select_value(
    select: Literal["all", "subset"] | list[str],
) -> None:
    left = pl.DataFrame({"id": ["a"], "x": [1]})
    right = pl.DataFrame({"id": ["a"], "x": [2]})
    primary_key = "id"
    comparison = compare_frames(left, right, primary_key=primary_key)
    with pytest.raises(ValueError, match="Invalid value for `select`"):
        comparison.joined_unequal(primary_key, select=select)


@pytest.mark.parametrize(
    ("perturbation", "abs_tol", "rel_tol"),
    [
        # NOTE: Double perturbations for tolerations to prevent odd floating point
        #  comparison issues
        (lambda col: col + 0.01, 0.02, 1e-05),
        (lambda col: col * 1.01, 1e-08, 0.02),
    ],
)
def test_float_tolerance_joined(
    perturbation: Callable[[pl.Expr], pl.Expr],
    abs_tol: float,
    rel_tol: float,
) -> None:
    base = pl.DataFrame({"id": ["a", "b", "c"], "x": [1.0, None, 3.0]})
    noisy = base.with_columns(perturbation(pl.col("x")))

    comparison_default = compare_frames(base, noisy, primary_key=["id"])
    assert comparison_default.num_rows_joined_equal() == 1  # NULL matches
    assert comparison_default.num_rows_joined_unequal() > 0

    comparison_lenient = compare_frames(
        base,
        noisy,
        primary_key=["id"],
        abs_tol=abs_tol,
        rel_tol=rel_tol,
    )
    assert comparison_lenient.num_rows_joined_unequal() == 0
    assert comparison_lenient.num_rows_joined_equal() > 0


@pytest.mark.parametrize(
    "abs_tol",
    [
        defaultdict(lambda: 0.01, {"value2": 0.2}),
        {"value": 0.01, "value2": 0.2},
    ],
    ids=["defaultdict", "dict"],
)
def test_float_abs_tol_mapping_joined(abs_tol: Mapping[str, float]) -> None:
    df = pl.DataFrame(
        data=[
            ("a", 2.0, 2.0),
            ("b", 3.0, 3.0),
            ("c", 4.0, 4.0),
        ],
        schema={
            "id": pl.String,
            "value": pl.Float64,
            "value2": pl.Float64,
        },
        orient="row",
    )
    df_pertubed = df.with_columns(pl.col("value") + 0.01, pl.col("value2") + 0.19)
    comparison = compare_frames(df, df_pertubed, primary_key="id")
    assert comparison.num_rows_joined_equal() == 0
    assert comparison.num_rows_joined_unequal() == 3

    comparison_lenient = compare_frames(
        df,
        df_pertubed,
        primary_key="id",
        abs_tol=abs_tol,
    )
    assert comparison_lenient.num_rows_joined_equal() == 3
    assert comparison_lenient.num_rows_joined_unequal() == 0


@pytest.mark.parametrize(
    "rtol",
    [
        defaultdict(lambda: 0.01, {"value2": 0.2}),
        {"value": 0.01, "value2": 0.2},
    ],
    ids=["defaultdict", "dict"],
)
def test_float_rtol_mapping_joined(rtol: Mapping[str, float]) -> None:
    df = pl.DataFrame(
        data=[
            ("a", 2.0, 2.0),
            ("b", 3.0, 3.0),
            ("c", 4.0, 4.0),
        ],
        schema={
            "id": pl.String,
            "value": pl.Float64,
            "value2": pl.Float64,
        },
        orient="row",
    )
    df_pertubed = df.with_columns(pl.col("value") * 1.01, pl.col("value2") * 1.19)
    comparison = compare_frames(df, df_pertubed, primary_key="id")
    assert comparison.num_rows_joined_equal() == 0
    assert comparison.num_rows_joined_unequal() == 3

    comparison_lenient = compare_frames(
        df,
        df_pertubed,
        primary_key="id",
        rel_tol=rtol,
    )
    assert comparison_lenient.num_rows_joined_equal() == 3
    assert comparison_lenient.num_rows_joined_unequal() == 0


@pytest.mark.parametrize(
    "abs_tol_temporal",
    [
        defaultdict(
            lambda: dt.timedelta(seconds=1), {"value2": dt.timedelta(seconds=2)}
        ),
        {"value": dt.timedelta(seconds=1), "value2": dt.timedelta(seconds=2)},
    ],
    ids=["defaultdict", "dict"],
)
def test_abs_tol_temporal_mapping_joined(
    abs_tol_temporal: Mapping[str, dt.timedelta],
) -> None:
    df = pl.DataFrame(
        data=[
            ("a", dt.datetime(2025, 1, 1, 0, 0, 0), dt.datetime(2025, 1, 1, 0, 0, 0)),
            ("b", dt.datetime(2025, 1, 2, 0, 0, 0), dt.datetime(2025, 1, 2, 0, 0, 0)),
            ("c", dt.datetime(2025, 1, 3, 0, 0, 0), dt.datetime(2025, 1, 3, 0, 0, 0)),
        ],
        schema={
            "id": pl.String,
            "value": pl.Datetime,
            "value2": pl.Datetime,
        },
        orient="row",
    )
    df_pertubed = df.with_columns(
        pl.col("value").dt.offset_by("1s"), pl.col("value2").dt.offset_by("2s")
    )
    comparison = compare_frames(df, df_pertubed, primary_key="id")
    assert comparison.num_rows_joined_equal() == 0
    assert comparison.num_rows_joined_unequal() == 3

    comparison_lenient = compare_frames(
        df,
        df_pertubed,
        primary_key="id",
        abs_tol_temporal=abs_tol_temporal,
    )
    assert comparison_lenient.num_rows_joined_equal() == 3
    assert comparison_lenient.num_rows_joined_unequal() == 0


def test_joined_equal_lazy_parameter() -> None:
    left = pl.DataFrame({"id": ["a", "b"], "x": [1, 2]})
    right = pl.DataFrame({"id": ["a", "b"], "x": [1, 99]})
    primary_key = ["id"]
    comparison = compare_frames(left, right, primary_key=primary_key)

    result_eager = comparison.joined_equal()
    assert isinstance(result_eager, pl.DataFrame)

    result_lazy = comparison.joined_equal(lazy=True)
    assert isinstance(result_lazy, pl.LazyFrame)

    assert_frame_equal(result_eager, result_lazy.collect(), check_row_order=False)


def test_joined_unequal_lazy_parameter() -> None:
    left = pl.DataFrame({"id": ["a", "b"], "x": [1, 2]})
    right = pl.DataFrame({"id": ["a", "b"], "x": [1, 99]})
    primary_key = ["id"]
    comparison = compare_frames(left, right, primary_key=primary_key)

    result_eager = comparison.joined_unequal()
    assert isinstance(result_eager, pl.DataFrame)

    result_lazy = comparison.joined_unequal(lazy=True)
    assert isinstance(result_lazy, pl.LazyFrame)

    assert_frame_equal(result_eager, result_lazy.collect(), check_row_order=False)
