# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import itertools
import json

import polars as pl
import pytest

from diffly import compare_frames
from diffly.comparison import DataFrameComparison
from diffly.summary import (
    SummaryData,
    SummaryDataColumn,
    SummaryDataColumnChange,
    SummaryDataRows,
    SummaryDataSchemas,
)


def _make_comparison() -> DataFrameComparison:
    """A rich comparison with schema diffs, row diffs, and column diffs."""
    left = pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "status": ["a", "b", "c", "d"],
            "value": [10.0, 20.0, 30.0, 40.0],
            "left_col": ["x", "y", "z", "w"],
        }
    )
    right = pl.DataFrame(
        {
            "id": [1, 2, 3, 5],
            "status": ["a", "x", "x", "e"],
            "value": [10.0, 25.0, 30.0, 50.0],
            "right_col": ["p", "q", "r", "s"],
        }
    )
    return compare_frames(left, right, primary_key="id")


@pytest.mark.parametrize(
    "show_perfect_column_matches, show_top_column_changes, slim, sample_rows, sample_pk",
    [
        (*combo[:2], combo[2], combo[3], combo[3] and combo[1])
        for combo in itertools.product([True, False], repeat=4)
    ],
)
def test_summary_data_parametrized(
    show_perfect_column_matches: bool,
    show_top_column_changes: bool,
    slim: bool,
    sample_rows: bool,
    sample_pk: bool,
) -> None:
    comp = _make_comparison()
    summary = comp.summary(
        show_perfect_column_matches=show_perfect_column_matches,
        top_k_column_changes=3 if show_top_column_changes else 0,
        slim=slim,
        sample_k_rows_only=3 if sample_rows else 0,
        show_sample_primary_key_per_change=sample_pk,
    )
    data = summary._data

    assert isinstance(data, SummaryData)
    assert data.equal is False
    assert data.primary_key == ["id"]

    # --- Schemas ---
    schemas_equal = comp.schemas.equal()
    if slim and schemas_equal:
        assert data.schemas is None
    else:
        assert isinstance(data.schemas, SummaryDataSchemas)
        assert len(data.schemas.left_only) > 0  # left_col
        assert len(data.schemas.right_only) > 0  # right_col

    # --- Rows ---
    rows_equal = comp._equal_rows()
    if slim and rows_equal:
        assert data.rows is None
    else:
        assert isinstance(data.rows, SummaryDataRows)
        assert data.rows.n_left == 4
        assert data.rows.n_right == 4
        assert data.rows.n_left_only is not None
        assert data.rows.n_right_only is not None

    # --- Columns ---
    assert data.columns is not None
    match_rates = comp.fraction_same()
    for col in data.columns:
        assert isinstance(col, SummaryDataColumn)
        rate = match_rates[col.name]
        assert col.match_rate == rate
        if show_top_column_changes and rate < 1:
            assert col.changes is not None
            for change in col.changes:
                assert isinstance(change, SummaryDataColumnChange)
                if sample_pk:
                    assert isinstance(change.sample_pk, tuple)
                    assert len(change.sample_pk) == 1
                else:
                    assert change.sample_pk is None
        else:
            assert col.changes is None

    # --- Sample rows ---
    if sample_rows:
        assert data.sample_rows_left_only is not None
        assert data.sample_rows_right_only is not None
        assert len(data.sample_rows_left_only) > 0
        assert len(data.sample_rows_right_only) > 0
        for row in data.sample_rows_left_only:
            assert isinstance(row, tuple)
        for row in data.sample_rows_right_only:
            assert isinstance(row, tuple)
    else:
        assert data.sample_rows_left_only is None
        assert data.sample_rows_right_only is None

    # JSON roundtrip
    parsed = json.loads(summary.to_json())
    assert isinstance(parsed, dict)
    assert parsed["equal"] is False


def test_summary_data_equal_frames() -> None:
    df = pl.DataFrame({"id": [1, 2], "value": [10.0, 20.0]})
    comp = compare_frames(df, df, primary_key="id")
    data = comp.summary()._data
    assert data.equal is True
    assert data.schemas is None
    assert data.rows is None
    assert data.columns is None
    assert data.sample_rows_left_only is None
    assert data.sample_rows_right_only is None


def test_summary_data_no_primary_key() -> None:
    left = pl.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
    right = pl.DataFrame({"a": [1, 2], "b": [3.0, 5.0]})
    comp = compare_frames(left, right)
    data = comp.summary()._data
    assert data.equal is False
    assert data.primary_key is None
    assert data.rows is not None
    assert data.rows.n_left_only is None
    assert data.rows.n_joined_equal is None
    assert data.columns is None
    assert data.sample_rows_left_only is None
    assert data.sample_rows_right_only is None


def test_summary_data_hidden_columns() -> None:
    left = pl.DataFrame({"id": [1, 2], "secret": ["a", "b"], "value": [10.0, 20.0]})
    right = pl.DataFrame({"id": [1, 2], "secret": ["a", "x"], "value": [10.0, 25.0]})
    comp = compare_frames(left, right, primary_key="id")
    data = comp.summary(
        top_k_column_changes=3,
        hidden_columns=["secret"],
    )._data
    assert data.columns is not None
    for col in data.columns:
        if col.name == "secret":
            assert col.changes is None
        elif col.match_rate < 1:
            assert col.changes is not None


def test_summary_data_validate_hidden_pk_sample_rows() -> None:
    df = pl.DataFrame({"id": ["a", "b", "c"]})
    comp = compare_frames(df, df.filter(pl.col("id") == "a"), primary_key=["id"])
    with pytest.raises(ValueError, match="Cannot show sample rows only"):
        comp.summary(sample_k_rows_only=3, hidden_columns=["id"])


def test_summary_data_validate_hidden_pk_sample_pk() -> None:
    df = pl.DataFrame({"id": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]})
    comp = compare_frames(df, df.with_columns(pl.col("value") + 1), primary_key=["id"])
    with pytest.raises(ValueError, match="Cannot show sample primary key"):
        comp.summary(
            top_k_column_changes=3,
            show_sample_primary_key_per_change=True,
            hidden_columns=["id"],
        )


def test_summary_data_validate_zero_top_k_with_sample_pk() -> None:
    df = pl.DataFrame({"id": ["a", "b"], "value": [1.0, 2.0]})
    comp = compare_frames(df, df.with_columns(pl.col("value") + 1), primary_key=["id"])
    with pytest.raises(
        ValueError,
        match="Cannot show sample primary key per change when top_k_column_changes is 0",
    ):
        comp.summary(top_k_column_changes=0, show_sample_primary_key_per_change=True)


def test_summary_data_multiple_pk_columns() -> None:
    left = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "val": [10, 20, 30]})
    right = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "val": [10, 99, 30]})
    comp = compare_frames(left, right, primary_key=["a", "b"])
    data = comp.summary(
        top_k_column_changes=3,
        show_sample_primary_key_per_change=True,
        sample_k_rows_only=3,
    )._data
    assert data.primary_key == ["a", "b"]
    assert data.columns is not None
    for col in data.columns:
        if col.changes:
            for change in col.changes:
                assert isinstance(change.sample_pk, tuple)
                assert len(change.sample_pk) == 2


def test_summary_data_to_dict() -> None:
    df = pl.DataFrame({"id": [1, 2], "value": [10.0, 20.0]})
    comp = compare_frames(df, df, primary_key="id")
    d = comp.summary()._data.to_dict()
    assert isinstance(d, dict)
    assert d["equal"] is True


def test_summary_data_slim_suppresses_matching_sections() -> None:
    left = pl.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
    right = pl.DataFrame({"id": [1, 2, 3], "value": [10.0, 25.0, 30.0]})
    comp = compare_frames(left, right, primary_key="id")
    data = comp.summary(slim=True)._data

    # Schemas match -> None in slim mode
    assert data.schemas is None
    # Rows have differences (joined unequal) -> shown
    assert data.rows is not None
    # Columns have differences -> shown
    assert data.columns is not None


def test_summary_data_n_total_changes() -> None:
    left = pl.DataFrame({"id": list(range(10)), "val": list(range(10))})
    right = pl.DataFrame({"id": list(range(10)), "val": list(range(10, 20))})
    comp = compare_frames(left, right, primary_key="id")
    data = comp.summary(top_k_column_changes=3)._data
    assert data.columns is not None
    col = next(c for c in data.columns if c.name == "val")
    assert col.changes is not None
    assert len(col.changes) == 3
    assert col.n_total_changes == 10
