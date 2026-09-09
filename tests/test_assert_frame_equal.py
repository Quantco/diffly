# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import textwrap

import polars as pl
import pytest

from diffly import compare_frames
from diffly.comparison import DataFrameComparison
from diffly.testing import (
    FrameComparisonAssertionError,
    assert_frame_equal,
)


def test_success_equal() -> None:
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
    assert_frame_equal(df, df)


@pytest.mark.parametrize("show_perfect_column_matches", [True, False])
@pytest.mark.parametrize("show_top_column_changes", [True, False])
def test_assertion_error_different(
    show_perfect_column_matches: bool,
    show_top_column_changes: bool,
) -> None:
    left = pl.DataFrame({"id": ["a", "b"], "value": [1, 2], "other": [10, 20]})
    right = pl.DataFrame(
        {"id": ["a", "b"], "value": [1, 3], "other": [10, 20], "extra": [100, 200]}
    )
    primary_key = ["id"]
    comparison = compare_frames(
        left,
        right,
        primary_key=primary_key,
    )
    summary = comparison.summary(
        show_perfect_column_matches=show_perfect_column_matches,
        top_k_column_changes=3 if show_top_column_changes else 0,
    )
    expected = (
        textwrap.indent(str(summary), " " * 2).replace("(", "\\(").replace(")", "\\)")
    )
    with pytest.raises(AssertionError, match=expected):
        assert_frame_equal(
            left,
            right,
            primary_key=primary_key,
            show_perfect_column_matches=show_perfect_column_matches,
            top_k_column_changes=3 if show_top_column_changes else 0,
        )


def test_assertion_dtype_mismatch() -> None:
    left = pl.DataFrame({"a": [1, 2]}, schema={"a": pl.Int64})
    right = pl.DataFrame({"a": [1, 2]}, schema={"a": pl.Int32})
    with pytest.raises(AssertionError):
        assert_frame_equal(left, right)
    assert_frame_equal(left, right, check_dtypes=False)


def test_success_with_nan() -> None:
    df = pl.DataFrame({"id": [1, 2], "value": [1.0, float("nan")]})
    assert_frame_equal(df, df)
    assert_frame_equal(df, df, primary_key="id")


@pytest.mark.parametrize(
    ("left_value", "right_value"),
    [
        (1.0, 1.0 + 1e-7),  # within 1e-05 rel_tol, outside 1e-09
        (0.0, 1e-17),  # within 1e-08 abs_tol, outside 0.0
    ],
)
def test_compare_frames_flags_what_assert_frame_equal_tolerates(
    left_value: float, right_value: float
) -> None:
    left = pl.DataFrame({"id": [1], "value": [left_value]})
    right = pl.DataFrame({"id": [1], "value": [right_value]})
    assert not compare_frames(left, right, primary_key="id").equal()
    assert_frame_equal(left, right, primary_key="id")  # must not raise


def test_error_exposes_comparison() -> None:
    # Arrange
    left = pl.DataFrame({"id": [1, 2], "value": [10.0, 20.0]})
    right = pl.DataFrame({"id": [1, 2], "value": [10.0, 25.0]})

    # Act
    with pytest.raises(FrameComparisonAssertionError) as exc_info:
        assert_frame_equal(left, right, primary_key="id")

    # Assert
    assert isinstance(exc_info.value, FrameComparisonAssertionError)
    comparison = exc_info.value.comparison
    assert isinstance(comparison, DataFrameComparison)
    assert comparison.fraction_same("value") == 0.5
