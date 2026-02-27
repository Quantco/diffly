# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import textwrap

import polars as pl
import pytest

from diffly import compare_frames
from diffly.testing import assert_frame_equal


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
