# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import itertools
import json

import polars as pl
import pytest

from diffly import compare_frames
from diffly.comparison import DataFrameComparison


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
    top_k = 3 if show_top_column_changes else 0
    summary = comp.summary(
        show_perfect_column_matches=show_perfect_column_matches,
        top_k_column_changes=top_k,
        sample_k_rows_only=3 if sample_rows else 0,
        show_sample_primary_key_per_change=sample_pk,
        slim=slim,
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
    # show_perfect_column_matches controls whether the perfect status column appears
    value_col = {
        "name": "value",
        "match_rate": pytest.approx(2 / 3),
        "n_total_changes": 1 if show_top_column_changes else 0,
        "changes": (
            [
                {
                    "old": 20.0,
                    "new": 25.0,
                    "count": 1,
                    "sample_pk": [2] if sample_pk else None,
                }
            ]
            if show_top_column_changes
            else None
        ),
    }
    expected_columns = []
    if show_perfect_column_matches:
        expected_columns.append(
            {"name": "status", "match_rate": 1.0, "n_total_changes": 0, "changes": None}
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
        "sample_rows_left_only": [[4]] if sample_rows else None,
        "sample_rows_right_only": [[5]] if sample_rows else None,
    }

    assert result == expected
