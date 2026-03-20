# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import datetime as dt

import polars as pl
import pytest

from diffly._conditions import _can_compare_dtypes, condition_equal_columns


def test_condition_equal_columns_struct() -> None:
    # Arrange
    lhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_left": [{"x": 1.0, "y": 2.0}, {"x": 2.0, "y": 2.1}],
        }
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_right": [{"y": 2.0, "x": 1.1}, {"y": 2.7, "x": 2.1}],
        }
    )

    # Act
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=None,
                abs_tol=0.5,
                rel_tol=0,
            )
        )
        .to_series()
    )

    # Assert
    assert actual.to_list() == [True, False]


def test_condition_equal_columns_different_struct_fields() -> None:
    # Arrange
    lhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_left": [{"x": 1.0, "z": 2.0}, {"x": 2.0, "z": 2.1}],
        }
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_right": [{"y": 2.0, "x": 1.1}, {"y": 2.7, "x": 2.1}],
        }
    )

    # Act
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=None,
            )
        )
        .to_series()
    )

    # Assert
    assert actual.to_list() == [False, False]


@pytest.mark.parametrize(
    "lhs_type", [pl.Array(pl.Float64, shape=2), pl.List(pl.Float64)]
)
@pytest.mark.parametrize(
    "rhs_type", [pl.Array(pl.Float64, shape=2), pl.List(pl.Float64)]
)
def test_condition_equal_columns_list_array_with_tolerance(
    lhs_type: pl.DataType, rhs_type: pl.DataType
) -> None:
    # Arrange
    lhs = pl.DataFrame(
        {
            "pk": [1, 2, 3],
            "a_left": [[1.0, 1.1], [2.0, 2.1], [3.0, 3.0]],
        },
        schema={"pk": pl.Int64, "a_left": lhs_type},
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2, 3],
            "a_right": [[1.0, 1.1], [2.0, 2.2], [3.0, 3.7]],
        },
        schema={"pk": pl.Int64, "a_right": rhs_type},
    )

    # Act
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                abs_tol=0.5,
                rel_tol=0,
                max_list_length=2,
            )
        )
        .to_series()
    )

    assert actual.to_list() == [True, True, False]


@pytest.mark.parametrize(
    "lhs_type",
    [pl.Array(pl.Float64, shape=(2, 3)), pl.List(pl.List(pl.Float64))],
)
@pytest.mark.parametrize(
    "rhs_type",
    [pl.Array(pl.Float64, shape=(2, 3)), pl.List(pl.List(pl.Float64))],
)
def test_condition_equal_columns_nested_list_array_with_tolerance(
    lhs_type: pl.DataType, rhs_type: pl.DataType
) -> None:
    # Arrange
    lhs = pl.DataFrame(
        {
            "pk": [1, 2, 3],
            "a_left": [
                [[1.0, 1.1, 1.3], [2.0, 2.1, 2.2]],
                [[3.0, 3.0, 3.1], [4.0, 4.0, 4.1]],
                [[5.0, 5.0, 5.1], [6.0, 6.0, 6.1]],
            ],
        },
        schema={"pk": pl.Int64, "a_left": lhs_type},
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2, 3],
            "a_right": [
                [[1.0, 1.1, 1.3], [2.0, 2.1, 2.2]],
                [[3.0, 3.0, 3.1], [4.0, 4.4, 4.1]],
                [[5.0, 5.0, 5.1], [6.0, 6.8, 6.1]],
            ],
        },
        schema={"pk": pl.Int64, "a_right": rhs_type},
    )

    # Act
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                abs_tol=0.5,
                rel_tol=0,
                max_list_length=2,
            )
        )
        .to_series()
    )

    if isinstance(lhs_type, pl.List) and isinstance(rhs_type, pl.List):
        assert actual.to_list() == [True, False, False]
    else:
        assert actual.to_list() == [True, True, False]


def test_condition_equal_columns_nested_dtype_mismatch() -> None:
    # Arrange
    lhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_left": [{"x": 1}, {"x": 2}],
        },
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_right": [[1.0, 1.1], [2.0, 2.2]],
        },
    )

    # Act
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=None,
            )
        )
        .to_series()
    )

    # Assert
    assert actual.to_list() == [False, False]


def test_condition_equal_columns_exactly_one_nested() -> None:
    # Arrange
    lhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_left": [{"x": 1}, {"x": 2}],
        },
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_right": [1, 2],
        },
    )

    # Act
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=None,
            )
        )
        .to_series()
    )

    # Assert
    assert actual.to_list() == [False, False]


def test_condition_equal_columns_temporal_tolerance() -> None:
    # Arrange
    lhs = pl.DataFrame(
        {
            "pk": [1, 2, 3, 4],
            "a_left": [
                dt.datetime(2025, 1, 1, 9, 0, 0),
                dt.datetime(2025, 1, 1, 10, 0, 0),
                None,
                None,
            ],
        },
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2, 3, 4],
            "a_right": [
                dt.datetime(2025, 1, 1, 9, 0, 1),
                dt.datetime(2025, 1, 1, 10, 0, 5),
                dt.datetime(2025, 1, 1, 10, 0, 0),
                None,
            ],
        },
    )

    # Act
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=None,
                abs_tol_temporal=dt.timedelta(seconds=2),
            )
        )
        .to_series()
    )

    # Assert
    assert actual.to_list() == [True, False, False, True]


def test_condition_equal_columns_two_lists() -> None:
    lhs = pl.DataFrame(
        {
            "pk": [1, 2, 3, 4, 5],
            "a_left": [[1.0, 2.0], [3.0], [5.0, None], None, None],
        },
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2, 3, 4, 5],
            "a_right": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0], [7.0], None],
        },
    )

    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                abs_tol=0.5,
                rel_tol=0,
                max_list_length=2,
            )
        )
        .to_series()
    )
    assert actual.to_list() == [True, False, False, False, True]


def test_condition_equal_columns_array_vs_list_length_mismatch() -> None:
    lhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_left": [[1.0, 2.0], [3.0, 4.0]],
        },
        schema={"pk": pl.Int64, "a_left": pl.Array(pl.Float64, shape=2)},
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_right": [[1.0, 2.0], [3.0]],
        },
    )

    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=None,
                abs_tol=0.5,
                rel_tol=0,
            )
        )
        .to_series()
    )
    assert actual.to_list() == [True, False]


def test_condition_equal_columns_two_arrays_different_shapes() -> None:
    lhs = pl.DataFrame(
        {
            "pk": [1],
            "a_left": [[1.0, 2.0]],
        },
        schema={"pk": pl.Int64, "a_left": pl.Array(pl.Float64, shape=2)},
    )
    rhs = pl.DataFrame(
        {
            "pk": [1],
            "a_right": [[1.0, 2.0, 3.0]],
        },
        schema={"pk": pl.Int64, "a_right": pl.Array(pl.Float64, shape=3)},
    )

    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=None,
            )
        )
        .to_series()
    )
    assert actual.to_list() == [False]


def test_condition_equal_columns_empty_arrays() -> None:
    lhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_left": [[], None],
        },
        schema={"pk": pl.Int64, "a_left": pl.Array(pl.Float64, shape=0)},
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_right": [[], None],
        },
        schema={"pk": pl.Int64, "a_right": pl.Array(pl.Float64, shape=0)},
    )

    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=None,
            )
        )
        .to_series()
    )
    assert actual.to_list() == [True, True]


def test_condition_equal_columns_empty_lists() -> None:
    lhs = pl.DataFrame(
        {
            "pk": [1, 2, 3],
            "a_left": [[], None, []],
        },
        schema={"pk": pl.Int64, "a_left": pl.List(pl.Float64)},
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2, 3],
            "a_right": [[], None, None],
        },
        schema={"pk": pl.Int64, "a_right": pl.List(pl.Float64)},
    )

    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=0,
            )
        )
        .to_series()
    )
    assert actual.to_list() == [True, True, False]


@pytest.mark.parametrize(
    ("dtype_left", "dtype_right", "can_compare_dtypes"),
    [
        (pl.Int64, pl.Float64, True),
        (pl.Datetime, pl.UInt16, False),
        (pl.Null, pl.Int16, True),
        (pl.Struct, pl.Float32, False),
        (pl.Struct, pl.Array, False),
        (pl.Struct, pl.Struct, True),
        (pl.List, pl.Array, True),
        (pl.Datetime, pl.Date, True),
        (pl.Boolean, pl.Boolean, True),
        (pl.Int64, pl.Boolean, True),
    ],
)
def test_can_compare_dtypes(
    dtype_left: pl.DataType, dtype_right: pl.DataType, can_compare_dtypes: bool
) -> None:
    can_compare_dtypes_actual = _can_compare_dtypes(
        dtype_left=dtype_left, dtype_right=dtype_right
    )
    assert can_compare_dtypes_actual == can_compare_dtypes
