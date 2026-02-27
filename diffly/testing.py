# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import datetime as dt
import textwrap
from collections.abc import Mapping, Sequence

import polars as pl

from diffly._utils import (
    ABS_TOL_DEFAULT,
    ABS_TOL_TEMPORAL_DEFAULT,
    REL_TOL_DEFAULT,
    Side,
)
from diffly.summary import WIDTH

from ._compat import dy
from .comparison import DataFrameComparison, compare_frames


def assert_collection_equal(
    left: dy.Collection,
    right: dy.Collection,
    /,
    *,
    check_dtypes: bool = True,
    abs_tol: float | Mapping[str, float] = ABS_TOL_DEFAULT,
    rel_tol: float | Mapping[str, float] = REL_TOL_DEFAULT,
    abs_tol_temporal: dt.timedelta
    | Mapping[str, dt.timedelta] = ABS_TOL_TEMPORAL_DEFAULT,
    show_perfect_column_matches: bool = False,
    top_k_column_changes: int = 0,
    sample_k_rows_only: int = 0,
    show_sample_primary_key_per_change: bool = False,
    left_name: str = Side.LEFT,
    right_name: str = Side.RIGHT,
    slim: bool = False,
    hidden_columns: list[str] | None = None,
) -> None:
    """Assert that two :mod:`dataframely` collections are equal.

    Two collections are considered equal if they are instances of the same collection
    type, have the same set of members, and all members are equal.

    Args:
        left: The first collection in the comparison.
        right: The second collection in the comparison.
        check_dtypes: Whether to check that the data types of columns match exactly.
        abs_tol: Absolute tolerance for comparing floating point types. If a
            :class:`Mapping` is provided, it should map from column name to absolute
            tolerance for every column in the data frame.
        rel_tol: Relative tolerance for comparing floating point types. If a
            :class:`Mapping` is provided, it should map from column name to relative
            tolerance for every column in the data frame.
        abs_tol_temporal: Absolute tolerance for comparing temporal types. If a
            :class:`Mapping` is provided, it should map from column name to absolute
            temporal tolerance for every column in the data frame.
        show_perfect_column_matches: Whether to include column matches in the assertion
            error even if the column match rate is 100%.
        top_k_column_changes: The maximum number of column values changes to display
            for columns with a match rate below 100% in the summary. When enabling this
            feature, make sure that no sensitive data is leaked.
        sample_k_rows_only: The number of rows to show in the "Rows left/right only"
            section of the summary. If 0 (default), no rows are shown. Only the
            primary key will be printed. An error will be raised if a positive
            number is provided and any of the primary key columns is also in
            `hidden_columns`.
        show_sample_primary_key_per_change: Whether to show a sample primary key per
            column change in the summary. If False (default), no primary key values
            are shown. A sample primary key can only be shown if
            `top_k_column_changes` is greater than 0, as each sample primary key is
            linked to a specific column change. An error will be raised if True and
            any of the primary key columns is also in `hidden_columns`.
        left_name: Custom display name for the left data frame.
        right_name: Custom display name for the right data frame.
        slim: Whether to generate a slim summary. In slim mode, the summary is as
            concise as possible, only showing sections that contain differences.
            As the structure of the summary can vary, it should only be used by
            advanced users who are familiar with the summary format.
        hidden_columns: Columns for which no values are printed, e.g. because they
            contain sensitive information.

    Raises:
        AssertionError: If the collections are not equal.
    """
    __tracebackhide__ = True

    if not type(left).matches(type(right)):
        raise AssertionError(
            "The collection definitions do not match.\n"
            f"Left: {type(left)}\nRight: {type(right)}"
        )
    if left.to_dict().keys() != right.to_dict().keys():
        raise AssertionError(
            "The collections have different members.\n"
            f"Left: {', '.join(left.to_dict().keys())}.\n"
            f"Right: {', '.join(right.to_dict().keys())}."
        )

    failed_member_comparisons: dict[str, DataFrameComparison] = {}
    for member_name in left.to_dict().keys():
        member_schema = left.member_schemas()[member_name]
        primary_key = member_schema.primary_key()
        comparison = compare_frames(
            left.to_dict()[member_name],
            right.to_dict()[member_name],
            primary_key=primary_key if len(primary_key) > 0 else None,
            abs_tol=abs_tol,
            rel_tol=rel_tol,
            abs_tol_temporal=abs_tol_temporal,
        )
        if not comparison.equal(check_dtypes=check_dtypes):
            failed_member_comparisons[member_name] = comparison

    if failed_member_comparisons:
        text = textwrap.indent(
            "\n\n".join(
                f"""{_get_heading(member)}\n{
                    str(
                        comparison.summary(
                            show_perfect_column_matches=show_perfect_column_matches,
                            top_k_column_changes=top_k_column_changes,
                            sample_k_rows_only=sample_k_rows_only,
                            show_sample_primary_key_per_change=(
                                show_sample_primary_key_per_change
                            ),
                            left_name=left_name,
                            right_name=right_name,
                            slim=slim,
                            hidden_columns=hidden_columns,
                        )
                    )
                }"""
                for member, comparison in failed_member_comparisons.items()
            ),
            " " * 2,
        )
        raise AssertionError(f"The following members are not equal:\n\n{text}")


def assert_frame_equal(
    left: pl.DataFrame | pl.LazyFrame,
    right: pl.DataFrame | pl.LazyFrame,
    /,
    *,
    primary_key: str | Sequence[str] | None = None,
    check_dtypes: bool = True,
    abs_tol: float | Mapping[str, float] = ABS_TOL_DEFAULT,
    rel_tol: float | Mapping[str, float] = REL_TOL_DEFAULT,
    abs_tol_temporal: dt.timedelta
    | Mapping[str, dt.timedelta] = ABS_TOL_TEMPORAL_DEFAULT,
    show_perfect_column_matches: bool = False,
    top_k_column_changes: int = 0,
    sample_k_rows_only: int = 0,
    show_sample_primary_key_per_change: bool = False,
    left_name: str = Side.LEFT,
    right_name: str = Side.RIGHT,
    slim: bool = False,
    hidden_columns: list[str] | None = None,
) -> None:
    """Assert that two :mod:`polars` data frames are equal.

    In contrast to :meth:`polars.testing.assert_frame_equal`, this method leverages
    :mod:`diffly`'s comparison logic. This allows printing a much more comprehensive
    summary of the changes between two data frames, making debugging considerably more
    straightforward.

    Args:
        left: The first data frame in the comparison.
        right: The second data frame in the comparison.
        primary_key: Primary key columns to use for joining the data frames. If not
            provided, the summary of the changes between two data frames are limited.
            Providing join columns does NOT have any functional effect on the assert of
            this function.
        check_dtypes: Whether to check that the data types of columns match exactly.
        abs_tol: Absolute tolerance for comparing floating point types. If a
            :class:`Mapping` is provided, it should map from column name to absolute
            tolerance for every column in the data frame (except the primary key).
        rel_tol: Relative tolerance for comparing floating point types. If a
            :class:`Mapping` is provided, it should map from column name to relative
            tolerance for every column in the data frame (except the primary key).
        abs_tol_temporal: Absolute tolerance for comparing temporal types. If a
            :class:`Mapping` is provided, it should map from column name to absolute
            temporal tolerance for every column in the data frame (except the primary
            key).
        show_perfect_column_matches: Whether to include column matches in the assertion
            error even if the column match rate is 100%.
        top_k_column_changes: The maximum number of column values changes to display
            for columns with a match rate below 100% in the summary. When enabling this
            feature, make sure that no sensitive data is leaked.
        sample_k_rows_only: The number of rows to show in the "Rows left/right only"
            section of the summary. If 0 (default), no rows are shown. Only the
            primary key will be printed. An error will be raised if a positive
            number is provided and any of the primary key columns is also in
            `hidden_columns`.
        show_sample_primary_key_per_change: Whether to show a sample primary key per
            column change in the summary. If False (default), no primary key values
            are shown. A sample primary key can only be shown if
            `top_k_column_changes` is greater than 0, as each sample primary key is
            linked to a specific column change. An error will be raised if True and
            any of the primary key columns is also in `hidden_columns`.
        left_name: Custom display name for the left data frame.
        right_name: Custom display name for the right data frame.
        slim: Whether to generate a slim summary. In slim mode, the summary is as
            concise as possible, only showing sections that contain differences.
            As the structure of the summary can vary, it should only be used by
            advanced users who are familiar with the summary format.
        hidden_columns: Columns for which no values are printed, e.g. because they
            contain sensitive information.

    Raises:
        AssertionError: If the data frames are not equal.

    Note:
        Contrary to :meth:`polars.testing.assert_frame_equal`, the data frames ``left``
        and ``right`` may both be either eager or lazy. They are not required to be the
        same for determining equivalence.
    """
    __tracebackhide__ = True

    comparison = compare_frames(
        left,
        right,
        primary_key=primary_key,
        abs_tol=abs_tol,
        rel_tol=rel_tol,
        abs_tol_temporal=abs_tol_temporal,
    )
    if not comparison.equal(check_dtypes=check_dtypes):
        summary = comparison.summary(
            show_perfect_column_matches=show_perfect_column_matches,
            top_k_column_changes=top_k_column_changes,
            sample_k_rows_only=sample_k_rows_only,
            show_sample_primary_key_per_change=show_sample_primary_key_per_change,
            left_name=left_name,
            right_name=right_name,
            slim=slim,
            hidden_columns=hidden_columns,
        )
        text = textwrap.indent(str(summary), " " * 2)
        raise AssertionError(f"Data frames are not equal:\n\n{text}")


def _get_heading(title: str) -> str:
    space_left = WIDTH - len(title) - 2
    left_pad = space_left // 2
    right_pad = space_left - left_pad
    return f" {'-' * (left_pad - 1)} {title.upper()} {'-' * (right_pad - 1)} "
