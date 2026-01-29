# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from collections.abc import Callable
from typing import Any

import polars as pl
import pytest

from diffly import compare_frames
from diffly.summary import _format_fraction_as_percentage


@pytest.mark.parametrize("show_perfect_column_matches", [True, False])
@pytest.mark.parametrize("show_top_column_changes", [True, False])
@pytest.mark.parametrize(("fn", "pretty"), [(str, False), (repr, True)])
def test_summary_dunder_methods(
    show_perfect_column_matches: bool,
    show_top_column_changes: bool,
    fn: Callable[[Any], str],
    pretty: bool,
) -> None:
    left = pl.DataFrame({"id": ["a", "b"], "x": [1, 2], "y": [3.0, 4.0]})
    right = pl.DataFrame(
        {"id": ["a", "b"], "x": [1, 99], "y": [3.0, 4.0], "z": ["new", "col"]}
    )
    primary_key = ["id"]
    comparison = compare_frames(left, right, primary_key=primary_key)
    summary = comparison.summary(
        show_perfect_column_matches=show_perfect_column_matches,
        top_k_column_changes=3 if show_top_column_changes else 0,
    )
    assert fn(summary) == summary.format(pretty=pretty)


@pytest.mark.parametrize("show_perfect_column_matches", [True, False])
@pytest.mark.parametrize("show_top_column_changes", [True, False])
def test_summary_all_columns_join_cols(
    show_perfect_column_matches: bool,
    show_top_column_changes: bool,
) -> None:
    # Arrange
    df = pl.DataFrame({"id": ["a", "b", "c"]})
    comp = compare_frames(
        df,
        df.filter(pl.col("id") == "a"),
        primary_key=["id"],
    )

    # Act
    summary = comp.summary(
        show_perfect_column_matches=show_perfect_column_matches,
        top_k_column_changes=3 if show_top_column_changes else 0,
    )

    # Assert
    assert "No common non-primary key columns to compare" in str(summary)


@pytest.mark.parametrize(
    "fraction, expected",
    [
        (0.5, "50.00%"),
        (0.123456, "12.35%"),
        (0.000001, "0.01%"),
        (0.999999, "99.99%"),
        (0, "0.00%"),
        (1, "100.00%"),
    ],
)
def test__format_fraction_as_percentage(fraction: float, expected: str) -> None:
    assert _format_fraction_as_percentage(fraction) == expected


def test_validate_primary_key_hidden_columns() -> None:
    df = pl.DataFrame({"id": ["a", "b", "c"]})
    comp = compare_frames(
        df,
        df.filter(pl.col("id") == "a"),
        primary_key=["id"],
    )

    with pytest.raises(
        ValueError, match="Cannot show sample rows only on the left or right*"
    ):
        _ = comp.summary(
            sample_k_rows_only=3,
            hidden_columns=["id"],
        )


def test_validate_primary_key_hidden_columns_with_sample_pk() -> None:
    df = pl.DataFrame({"id": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]})
    comp = compare_frames(
        df,
        df.with_columns(pl.col("value") + 1),
        primary_key=["id"],
    )

    with pytest.raises(
        ValueError,
        match="Cannot show sample primary key for changed columns when primary key column",
    ):
        _ = comp.summary(
            top_k_column_changes=3,
            show_sample_primary_key_per_change=True,
            hidden_columns=["id"],
        )


def test_zero_top_k_column_changes_with_show_sample_primary_key() -> None:
    df = pl.DataFrame({"id": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]})
    comp = compare_frames(
        df,
        df.with_columns(pl.col("value") + 1),
        primary_key=["id"],
    )

    with pytest.raises(
        ValueError,
        match="Cannot show sample primary key per change when top_k_column_changes is 0.",
    ):
        _ = comp.summary(
            top_k_column_changes=0,
            show_sample_primary_key_per_change=True,
        )
