# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import datetime as dt

import polars as pl
import pytest

from diffly._conditions import _can_compare_dtypes, condition_equal_columns
from diffly.comparison import compare_frames


def test_condition_equal_columns_struct() -> None:
    # Arrange
    lhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a": [{"x": 1.0, "y": 2.0}, {"x": 2.0, "y": 2.1}],
        }
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a": [{"y": 2.0, "x": 1.1}, {"y": 2.7, "x": 2.1}],
        }
    )
    c = compare_frames(lhs, rhs, primary_key="pk", abs_tol=0.5, rel_tol=0)

    # Act
    lhs = lhs.rename({"a": "a_left"})
    rhs = rhs.rename({"a": "a_right"})
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=c._max_list_lengths_by_column.get("a"),
                abs_tol=c.abs_tol_by_column["a"],
                rel_tol=c.rel_tol_by_column["a"],
            )
        )
        .to_series()
    )

    # Assert
    assert c._max_list_lengths_by_column == {}
    assert actual.to_list() == [True, False]


def test_condition_equal_columns_different_struct_fields() -> None:
    # Arrange
    lhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a": [{"x": 1.0, "z": 2.0}, {"x": 2.0, "z": 2.1}],
        }
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a": [{"y": 2.0, "x": 1.1}, {"y": 2.7, "x": 2.1}],
        }
    )
    c = compare_frames(lhs, rhs, primary_key="pk")

    # Act
    lhs = lhs.rename({"a": "a_left"})
    rhs = rhs.rename({"a": "a_right"})
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=c._max_list_lengths_by_column.get("a"),
                abs_tol=c.abs_tol_by_column["a"],
                rel_tol=c.rel_tol_by_column["a"],
            )
        )
        .to_series()
    )

    # Assert
    assert c._max_list_lengths_by_column == {}
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
        {"pk": [1, 2, 3], "a": [[1.0, 1.1], [2.0, 2.1], [3.0, 3.0]]},
        schema={"pk": pl.Int64, "a": lhs_type},
    )
    rhs = pl.DataFrame(
        {"pk": [1, 2, 3], "a": [[1.0, 1.1], [2.0, 2.2], [3.0, 3.7]]},
        schema={"pk": pl.Int64, "a": rhs_type},
    )
    c = compare_frames(lhs, rhs, primary_key="pk", abs_tol=0.5, rel_tol=0)

    # Act
    lhs = lhs.rename({"a": "a_left"})
    rhs = rhs.rename({"a": "a_right"})
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=c._max_list_lengths_by_column.get("a"),
                abs_tol=c.abs_tol_by_column["a"],
                rel_tol=c.rel_tol_by_column["a"],
            )
        )
        .to_series()
    )

    # Assert
    if isinstance(lhs_type, pl.List) and isinstance(rhs_type, pl.List):
        assert c._max_list_lengths_by_column == {"a": 2}
    else:
        assert c._max_list_lengths_by_column == {}
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
            "a": [
                [[1.0, 1.1, 1.3], [2.0, 2.1, 2.2]],
                [[3.0, 3.0, 3.1], [4.0, 4.0, 4.1]],
                [[5.0, 5.0, 5.1], [6.0, 6.0, 6.1]],
            ],
        },
        schema={"pk": pl.Int64, "a": lhs_type},
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2, 3],
            "a": [
                [[1.0, 1.1, 1.3], [2.0, 2.1, 2.2]],
                [[3.0, 3.0, 3.1], [4.0, 4.4, 4.1]],
                [[5.0, 5.0, 5.1], [6.0, 6.8, 6.1]],
            ],
        },
        schema={"pk": pl.Int64, "a": rhs_type},
    )
    c = compare_frames(lhs, rhs, primary_key="pk", abs_tol=0.5, rel_tol=0)

    # Act
    lhs = lhs.rename({"a": "a_left"})
    rhs = rhs.rename({"a": "a_right"})
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=c._max_list_lengths_by_column.get("a"),
                abs_tol=c.abs_tol_by_column["a"],
                rel_tol=c.rel_tol_by_column["a"],
            )
        )
        .to_series()
    )

    # Assert
    if isinstance(lhs_type, pl.List) and isinstance(rhs_type, pl.List):
        assert c._max_list_lengths_by_column == {"a": 3}
    else:
        assert c._max_list_lengths_by_column == {}
    assert actual.to_list() == [True, True, False]


def test_condition_equal_columns_nested_dtype_mismatch() -> None:
    # Arrange
    lhs = pl.DataFrame({"pk": [1, 2], "a": [{"x": 1}, {"x": 2}]})
    rhs = pl.DataFrame({"pk": [1, 2], "a": [[1.0, 1.1], [2.0, 2.2]]})
    c = compare_frames(lhs, rhs, primary_key="pk")

    # Act
    lhs = lhs.rename({"a": "a_left"})
    rhs = rhs.rename({"a": "a_right"})
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=c._max_list_lengths_by_column.get("a"),
                abs_tol=c.abs_tol_by_column["a"],
                rel_tol=c.rel_tol_by_column["a"],
            )
        )
        .to_series()
    )

    # Assert
    assert c._max_list_lengths_by_column == {}
    assert actual.to_list() == [False, False]


def test_condition_equal_columns_exactly_one_nested() -> None:
    # Arrange
    lhs = pl.DataFrame({"pk": [1, 2], "a": [{"x": 1}, {"x": 2}]})
    rhs = pl.DataFrame({"pk": [1, 2], "a": [1, 2]})
    c = compare_frames(lhs, rhs, primary_key="pk")

    # Act
    lhs = lhs.rename({"a": "a_left"})
    rhs = rhs.rename({"a": "a_right"})
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=c._max_list_lengths_by_column.get("a"),
                abs_tol=c.abs_tol_by_column["a"],
                rel_tol=c.rel_tol_by_column["a"],
            )
        )
        .to_series()
    )

    # Assert
    assert c._max_list_lengths_by_column == {}
    assert actual.to_list() == [False, False]


def test_condition_equal_columns_temporal_tolerance() -> None:
    # Arrange
    lhs = pl.DataFrame(
        {
            "pk": [1, 2, 3, 4],
            "a": [
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
            "a": [
                dt.datetime(2025, 1, 1, 9, 0, 1),
                dt.datetime(2025, 1, 1, 10, 0, 5),
                dt.datetime(2025, 1, 1, 10, 0, 0),
                None,
            ],
        },
    )
    c = compare_frames(
        lhs, rhs, primary_key="pk", abs_tol_temporal=dt.timedelta(seconds=2)
    )

    # Act
    lhs = lhs.rename({"a": "a_left"})
    rhs = rhs.rename({"a": "a_right"})
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=c._max_list_lengths_by_column.get("a"),
                abs_tol=c.abs_tol_by_column["a"],
                rel_tol=c.rel_tol_by_column["a"],
                abs_tol_temporal=c.abs_tol_temporal_by_column["a"],
            )
        )
        .to_series()
    )

    # Assert
    assert c._max_list_lengths_by_column == {}
    assert actual.to_list() == [True, False, False, True]


def test_condition_equal_columns_two_lists() -> None:
    # Arrange
    lhs = pl.DataFrame(
        {"pk": [1, 2, 3, 4, 5], "a": [[1.0, 2.0], [3.0], [5.0, None], None, None]},
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2, 3, 4, 5],
            "a": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0], [7.0], None],
        },
    )
    c = compare_frames(lhs, rhs, primary_key="pk", abs_tol=0.5, rel_tol=0)

    # Act
    lhs = lhs.rename({"a": "a_left"})
    rhs = rhs.rename({"a": "a_right"})
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=c._max_list_lengths_by_column.get("a"),
                abs_tol=c.abs_tol_by_column["a"],
                rel_tol=c.rel_tol_by_column["a"],
            )
        )
        .to_series()
    )

    # Assert
    assert c._max_list_lengths_by_column == {"a": 2}
    assert actual.to_list() == [True, False, False, False, True]


def test_condition_equal_columns_array_vs_list_length_mismatch() -> None:
    # Arrange
    lhs = pl.DataFrame(
        {"pk": [1, 2], "a": [[1.0, 2.0], [3.0, 4.0]]},
        schema={"pk": pl.Int64, "a": pl.Array(pl.Float64, shape=2)},
    )
    rhs = pl.DataFrame({"pk": [1, 2], "a": [[1.0, 2.0], [3.0]]})
    c = compare_frames(lhs, rhs, primary_key="pk", abs_tol=0.5, rel_tol=0)

    # Act
    lhs = lhs.rename({"a": "a_left"})
    rhs = rhs.rename({"a": "a_right"})
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=c._max_list_lengths_by_column.get("a"),
                abs_tol=c.abs_tol_by_column["a"],
                rel_tol=c.rel_tol_by_column["a"],
            )
        )
        .to_series()
    )

    # Assert
    assert c._max_list_lengths_by_column == {}
    assert actual.to_list() == [True, False]


def test_condition_equal_columns_two_arrays_different_shapes() -> None:
    # Arrange
    lhs = pl.DataFrame(
        {"pk": [1], "a": [[1.0, 2.0]]},
        schema={"pk": pl.Int64, "a": pl.Array(pl.Float64, shape=2)},
    )
    rhs = pl.DataFrame(
        {"pk": [1], "a": [[1.0, 2.0, 3.0]]},
        schema={"pk": pl.Int64, "a": pl.Array(pl.Float64, shape=3)},
    )
    c = compare_frames(lhs, rhs, primary_key="pk")

    # Act
    lhs = lhs.rename({"a": "a_left"})
    rhs = rhs.rename({"a": "a_right"})
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=c._max_list_lengths_by_column.get("a"),
                abs_tol=c.abs_tol_by_column["a"],
                rel_tol=c.rel_tol_by_column["a"],
            )
        )
        .to_series()
    )

    # Assert
    assert c._max_list_lengths_by_column == {}
    assert actual.to_list() == [False]


@pytest.mark.parametrize(
    "lhs_type", [pl.Array(pl.Float64, shape=0), pl.List(pl.Float64)]
)
@pytest.mark.parametrize(
    "rhs_type", [pl.Array(pl.Float64, shape=0), pl.List(pl.Float64)]
)
def test_condition_equal_columns_empty_list_array(
    lhs_type: pl.DataType, rhs_type: pl.DataType
) -> None:
    # Arrange
    lhs = pl.DataFrame(
        {"pk": [1, 2], "a": [[], None]},
        schema={"pk": pl.Int64, "a": lhs_type},
    )
    rhs = pl.DataFrame(
        {"pk": [1, 2], "a": [[], None]},
        schema={"pk": pl.Int64, "a": rhs_type},
    )
    c = compare_frames(lhs, rhs, primary_key="pk")

    # Act
    lhs = lhs.rename({"a": "a_left"})
    rhs = rhs.rename({"a": "a_right"})
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=c._max_list_lengths_by_column.get("a"),
                abs_tol=c.abs_tol_by_column["a"],
                rel_tol=c.rel_tol_by_column["a"],
            )
        )
        .to_series()
    )

    # Assert
    if isinstance(lhs_type, pl.List) and isinstance(rhs_type, pl.List):
        assert c._max_list_lengths_by_column == {"a": 0}
    else:
        assert c._max_list_lengths_by_column == {}
    assert actual.to_list() == [True, True]


def test_condition_equal_columns_lists_only_inner() -> None:
    # Arrange
    lhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a": [
                {
                    "x": 1,
                    "y": [1.0, 2.0, 3.0],
                },
                {
                    "x": 2,
                    "y": [4.0, 5.0, 6.0],
                },
            ],
        },
    )
    rhs = pl.DataFrame(
        {
            "pk": [1, 2],
            "a": [
                {
                    "x": 1,
                    "y": [1.0, 2.1, 3.0],
                },
                {
                    "x": 2,
                    "y": [4.0, 5.3, 6.0],
                },
            ],
        },
    )
    c = compare_frames(lhs, rhs, primary_key="pk", abs_tol=0.2, rel_tol=0)

    # Act
    lhs = lhs.rename({"a": "a_left"})
    rhs = rhs.rename({"a": "a_right"})
    actual = (
        lhs.join(rhs, on="pk", maintain_order="left")
        .select(
            condition_equal_columns(
                "a",
                dtype_left=lhs.schema["a_left"],
                dtype_right=rhs.schema["a_right"],
                max_list_length=c._max_list_lengths_by_column.get("a"),
                abs_tol=c.abs_tol_by_column["a"],
                rel_tol=c.rel_tol_by_column["a"],
            )
        )
        .to_series()
    )

    # Assert
    assert c._max_list_lengths_by_column == {"a": 3}
    assert actual.to_list() == [True, False]


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
