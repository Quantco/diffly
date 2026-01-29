# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import datetime as dt
from collections.abc import Mapping

import polars as pl
from polars.datatypes import DataType, DataTypeClass

from diffly._utils import (
    ABS_TOL_DEFAULT,
    ABS_TOL_TEMPORAL_DEFAULT,
    REL_TOL_DEFAULT,
    Side,
)


def condition_equal_rows(
    columns: list[str],
    schema_left: pl.Schema,
    schema_right: pl.Schema,
    abs_tol_by_column: Mapping[str, float],
    rel_tol_by_column: Mapping[str, float],
    abs_tol_temporal_by_column: Mapping[str, dt.timedelta],
) -> pl.Expr:
    """Build an expression whether two rows are equal, based on all columns' data
    types."""
    if not columns:
        return pl.lit(True)

    return pl.all_horizontal(
        [
            condition_equal_columns(
                column=column,
                dtype_left=schema_left[column],
                dtype_right=schema_right[column],
                abs_tol=abs_tol_by_column[column],
                rel_tol=rel_tol_by_column[column],
                abs_tol_temporal=abs_tol_temporal_by_column[column],
            )
            for column in columns
        ]
    )


def condition_equal_columns(
    column: str,
    dtype_left: pl.DataType,
    dtype_right: pl.DataType,
    abs_tol: float = ABS_TOL_DEFAULT,
    rel_tol: float = REL_TOL_DEFAULT,
    abs_tol_temporal: dt.timedelta = ABS_TOL_TEMPORAL_DEFAULT,
) -> pl.Expr:
    """Build an expression whether two columns are equal, depending on the columns' data
    types."""
    return _compare_columns(
        col_left=pl.col(f"{column}_{Side.LEFT}"),
        col_right=pl.col(f"{column}_{Side.RIGHT}"),
        dtype_left=dtype_left,
        dtype_right=dtype_right,
        abs_tol=abs_tol,
        rel_tol=rel_tol,
        abs_tol_temporal=abs_tol_temporal,
    )


# --------------------------------------- UTILS -------------------------------------- #


def _can_compare_dtypes(
    dtype_left: DataType | DataTypeClass,
    dtype_right: DataType | DataTypeClass,
) -> bool:
    return (
        (dtype_left == dtype_right)
        or (dtype_left == pl.Null)
        or (dtype_right == pl.Null)
        or (
            (
                (dtype_left.is_numeric() or dtype_left == pl.Boolean)
                == (dtype_right.is_numeric() or dtype_right == pl.Boolean)
            )
            and (dtype_left.is_temporal() == dtype_right.is_temporal())
            and (dtype_left.is_nested() == dtype_right.is_nested())
            and ((dtype_left == pl.Struct) == (dtype_right == pl.Struct))
        )
    )


def _compare_columns(
    col_left: pl.Expr,
    col_right: pl.Expr,
    dtype_left: DataType | DataTypeClass,
    dtype_right: DataType | DataTypeClass,
    abs_tol: float,
    rel_tol: float,
    abs_tol_temporal: dt.timedelta,
) -> pl.Expr:
    """Build an expression whether two expressions yield the same value.

    This method is more generic than :meth:`condition_equal_columns` as it accepts two
    arbitrary expressions rather than a "base column name".
    """
    if not _can_compare_dtypes(dtype_left, dtype_right):
        return pl.repeat(pl.lit(False), pl.len())

    # If we encounter nested dtypes, we have to treat them specially
    if dtype_left.is_nested():
        if isinstance(dtype_left, pl.Struct):
            assert isinstance(dtype_right, pl.Struct)
            # For two structs, we necessarily need to have matching field names (the
            # order does not matter). If that isn't the case, we cannot observe equality
            fields_left = {f.name: f.dtype for f in dtype_left.fields}
            fields_right = {f.name: f.dtype for f in dtype_right.fields}
            if fields_left.keys() != fields_right.keys():
                return pl.repeat(pl.lit(False), pl.len())

            # Otherwise, we simply compare all fields independently
            return pl.all_horizontal(
                [
                    _compare_columns(
                        col_left=col_left.struct[field],
                        col_right=col_right.struct[field],
                        dtype_left=fields_left[field],
                        dtype_right=fields_right[field],
                        abs_tol=abs_tol,
                        rel_tol=rel_tol,
                        abs_tol_temporal=abs_tol_temporal,
                    )
                    for field in fields_left
                ]
            )
        elif isinstance(dtype_left, pl.List | pl.Array) and isinstance(
            dtype_right, pl.List | pl.Array
        ):
            # As of polars 1.28, there is no way to access another column within
            # `list.eval`. Hence, we necessarily need to resort to a primitive
            # comparison in this case.
            pass

    if (
        isinstance(dtype_left, pl.Enum)
        and isinstance(dtype_right, pl.Enum)
        and dtype_left != dtype_right
    ) or _enum_and_categorical(dtype_left, dtype_right):
        # Enums with different categories as well as enums and categoricals
        # can't be compared directly.
        # Fall back to comparison of strings.
        return _compare_columns(
            col_left=col_left.cast(pl.String),
            col_right=col_right.cast(pl.String),
            dtype_left=pl.String,
            dtype_right=pl.String,
            abs_tol=abs_tol,
            rel_tol=rel_tol,
            abs_tol_temporal=abs_tol_temporal,
        )

    return _compare_primitive_columns(
        col_left=col_left,
        col_right=col_right,
        dtype_left=dtype_left,
        dtype_right=dtype_right,
        abs_tol=abs_tol,
        rel_tol=rel_tol,
        abs_tol_temporal=abs_tol_temporal,
    )


def _compare_primitive_columns(
    col_left: pl.Expr,
    col_right: pl.Expr,
    dtype_left: DataType | DataTypeClass,
    dtype_right: DataType | DataTypeClass,
    abs_tol: float,
    rel_tol: float,
    abs_tol_temporal: dt.timedelta,
) -> pl.Expr:
    if (dtype_left.is_float() or dtype_right.is_float()) and (
        dtype_left.is_numeric() and dtype_right.is_numeric()
    ):
        return col_left.is_close(col_right, abs_tol=abs_tol, rel_tol=rel_tol).pipe(
            _eq_missing_with_nan, lhs=col_left, rhs=col_right
        )
    elif dtype_left.is_temporal() and dtype_right.is_temporal():
        diff_less_than_tolerance = (col_left - col_right).abs() <= abs_tol_temporal
        return diff_less_than_tolerance.pipe(_eq_missing, lhs=col_left, rhs=col_right)

    return col_left.eq_missing(col_right)


def _eq_missing(expr: pl.Expr, lhs: pl.Expr, rhs: pl.Expr) -> pl.Expr:
    both_null = lhs.is_null() & rhs.is_null()
    both_not_null = lhs.is_not_null() & rhs.is_not_null()
    return (expr & both_not_null) | both_null


def _eq_missing_with_nan(expr: pl.Expr, lhs: pl.Expr, rhs: pl.Expr) -> pl.Expr:
    both_nan = lhs.is_nan() & rhs.is_nan()
    return _eq_missing(expr, lhs, rhs) | both_nan


def _enum_and_categorical(
    left: DataType | DataTypeClass, right: DataType | DataTypeClass
) -> bool:
    return (isinstance(left, pl.Enum) and isinstance(right, pl.Categorical)) or (
        isinstance(left, pl.Categorical) and isinstance(right, pl.Enum)
    )
