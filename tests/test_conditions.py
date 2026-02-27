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
def test_condition_equal_columns_list_array_equal_exact(
    lhs_type: pl.DataType, rhs_type: pl.DataType
) -> None:
    # Arrange
    lhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_left": [[1.0, 1.1], [2.0, 2.1]],
        },
        schema={"pk": pl.Int64, "a_left": lhs_type},
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a_right": [[1.0, 1.1], [2.0, 2.2]],
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
            )
        )
        .to_series()
    )

    # Assert
    assert actual.to_list() == [True, False]


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
                abs_tol_temporal=dt.timedelta(seconds=2),
            )
        )
        .to_series()
    )

    # Assert
    assert actual.to_list() == [True, False, False, True]


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
