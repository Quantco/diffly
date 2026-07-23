# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import itertools
import json
from collections.abc import Callable
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import polars as pl
import pytest

from diffly import compare_frames, metrics
from diffly.comparison import DataFrameComparison
from diffly.metrics.data import DEFAULT_DATA_METRICS, DataMetric
from diffly.summary import _format_fraction_as_percentage, to_json_safe


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


def test_change_and_data_metrics_routed_to_separate_fields() -> None:
    # Joined rows id=1,2,3. value deltas (right - left) = [0, 5, null] → Mean = 2.5.
    # value nulls: left 0/3 = 0%, right 1/3 = 33.33% → Null% = "0.0% -> 33.33% (+33.33)".
    left = pl.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
    right = pl.DataFrame({"id": [1, 2, 3], "value": [10.0, 25.0, None]})
    comp = compare_frames(left, right, primary_key="id")

    summary = comp.summary(
        metrics={"Mean": metrics.change.mean, "Null%": DEFAULT_DATA_METRICS["Null%"]},
    )
    result = json.loads(summary.to_json())

    (value_col,) = result["columns"]
    assert value_col["name"] == "value"
    # Change metric lands in the column's `change_metrics`, data metric in the separate
    # `data_inspection` section.
    assert value_col["change_metrics"] == {"Mean": pytest.approx(2.5)}
    assert "data_metrics" not in value_col
    (value_inspection,) = result["data_inspection"]
    assert value_inspection["name"] == "value"
    assert value_inspection["data_metrics"] == {
        "Null%": {"left": pytest.approx(0.0), "right": pytest.approx(1 / 3)}
    }


def test_data_metrics_consider_unjoined_rows() -> None:
    # Joined rows are id=1,2,3. The extreme `value`s live in unjoined rows: id=4 is
    # left-only (999.0) and id=5 is right-only (888.0). A data metric that only looked at
    # the joined rows would report max 30.0 on both sides, so the metric picking these up
    # proves it considers values from unjoined rows.
    left = pl.DataFrame({"id": [1, 2, 3, 4], "value": [10.0, 20.0, 30.0, 999.0]})
    right = pl.DataFrame({"id": [1, 2, 3, 5], "value": [10.0, 25.0, 30.0, 888.0]})
    comp = compare_frames(left, right, primary_key="id")

    summary = comp.summary(metrics={"Max": DataMetric(fn=lambda col: col.max())})
    result = json.loads(summary.to_json())

    (value_inspection,) = result["data_inspection"]
    assert value_inspection["name"] == "value"
    assert value_inspection["data_metrics"] == {"Max": {"left": 999.0, "right": 888.0}}


def _make_comparison() -> DataFrameComparison:
    # Designed so every parametrized flag affects the expected JSON output:
    # - Same columns in both frames → schemas equal → slim suppresses schemas section
    # - status matches perfectly for joined rows → show_perfect_column_matches matters
    # - value differs for id=2 → always has a non-perfect column
    # - id=4 left-only, id=5 right-only → sample rows matter
    left = pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "status": ["a", "b", "c", "d"],
            "value": [10.0, 20.0, 30.0, 40.0],
        }
    )
    right = pl.DataFrame(
        {
            "id": [1, 2, 3, 5],
            "status": ["a", "b", "c", "e"],
            "value": [10.0, 25.0, 30.0, 50.0],
        }
    )
    return compare_frames(left, right, primary_key="id")


@pytest.mark.parametrize(
    "show_perfect_column_matches, show_top_column_changes, slim, sample_rows, sample_pk, hide_value, with_metrics",
    [
        (
            combo[0],
            combo[1],
            combo[2],
            combo[3],
            combo[3] and combo[1],
            combo[4],
            combo[5],
        )
        for combo in itertools.product([True, False], repeat=6)
    ],
)
def test_summary_data_parametrized(
    show_perfect_column_matches: bool,
    show_top_column_changes: bool,
    slim: bool,
    sample_rows: bool,
    sample_pk: bool,
    hide_value: bool,
    with_metrics: bool,
) -> None:
    comp = _make_comparison()
    top_k = 3 if show_top_column_changes else 0
    hidden_columns = ["value"] if hide_value else None
    metrics_arg = (
        {"Mean": metrics.change.mean, "Max": metrics.change.max}
        if with_metrics
        else None
    )
    summary = comp.summary(
        show_perfect_column_matches=show_perfect_column_matches,
        top_k_column_changes=top_k,
        sample_k_rows_only=3 if sample_rows else 0,
        show_sample_primary_key_per_change=sample_pk,
        slim=slim,
        hidden_columns=hidden_columns,
        metrics=metrics_arg,
    )
    result = json.loads(summary.to_json())

    # --- Build expected dictionary ---
    # Schemas: equal (same columns, same dtypes) → suppressed in slim mode
    expected_schemas: dict | None = None
    if not slim:
        expected_schemas = {
            "left_only_names": [],
            "in_common": [
                ["id", "Int64", "Int64"],
                ["status", "String", "String"],
                ["value", "Float64", "Float64"],
            ],
            "right_only_names": [],
        }

    # Columns: status has 100% match rate, value has 2/3
    # - show_perfect_column_matches controls whether the perfect status column appears
    # - hide_value suppresses changes for value (top_k forced to 0 for hidden columns)
    show_value_changes = show_top_column_changes and not hide_value
    value_col = {
        "name": "value",
        "match_rate": pytest.approx(2 / 3),
        "n_total_changes": 1 if show_value_changes else 0,
        "changes": (
            [
                {
                    "old": 20.0,
                    "new": 25.0,
                    "count": 1,
                    "sample_pk": [2] if sample_pk else None,
                }
            ]
            if show_value_changes
            else None
        ),
        # Joined rows (id=1,2,3): value deltas = [0, 5, 0].
        "change_metrics": {"Mean": pytest.approx(5 / 3), "Max": 5.0}
        if with_metrics
        else None,
    }
    expected_columns = []
    if show_perfect_column_matches:
        expected_columns.append(
            {
                "name": "status",
                "match_rate": 1.0,
                "n_total_changes": 0,
                "changes": None,
                "change_metrics": None,
            }
        )
    expected_columns.append(value_col)

    expected = {
        "equal": False,
        "left_name": "left",
        "right_name": "right",
        "primary_key": ["id"],
        "schemas": expected_schemas,
        "rows": {
            "n_left": 4,
            "n_right": 4,
            "n_left_only": 1,
            "n_joined_equal": 2,
            "n_joined_unequal": 1,
            "n_right_only": 1,
        },
        "columns": expected_columns,
        # Only change metrics are supplied, so the data inspection section is absent.
        "data_inspection": None,
        "sample_rows_left_only": [[4]] if sample_rows else None,
        "sample_rows_right_only": [[5]] if sample_rows else None,
    }

    assert result == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ([1, 2, 3], [1, 2, 3]),
        ({"a": 1, "b": 2}, {"a": 1, "b": 2}),
        ("string", "string"),
        (123, 123),
        (12.34, 12.34),
        (True, True),
        (None, None),
        (date(2024, 1, 1), "2024-01-01"),
        (datetime(2024, 1, 1, 12, 0, 0), "2024-01-01T12:00:00"),
        (Decimal("12.34"), 12.34),
        (timedelta(hours=1, minutes=30), 5400),
    ],
)
def test_to_json_safe(input: Any, expected: Any) -> None:
    assert to_json_safe(input) == expected
