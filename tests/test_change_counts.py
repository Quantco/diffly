# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from diffly import compare_frames


@pytest.mark.parametrize("include_sample_primary_key", [True, False])
def test_change_counts_single_primary_key_column(
    include_sample_primary_key: bool,
) -> None:
    left = pl.DataFrame(
        {
            "name": ["cat", "dog", "mouse", "bird"],
            "age": [10, 20, 20, 30],
        }
    )
    right = pl.DataFrame(
        {
            "name": ["cat", "dog", "mouse", "bird"],
            "age": [10, None, None, None],
        }
    )
    comparison = compare_frames(left, right, primary_key=["name"])

    expected = pl.DataFrame(
        data={
            "left": [20, 30],
            "right": [None, None],
            "count": [2, 1],
            "sample_name": ["dog", "bird"],
        },
        schema={
            "left": pl.Int64,
            "right": pl.Int64,
            "count": pl.UInt32,
            "sample_name": pl.String,
        },
    )

    if not include_sample_primary_key:
        expected = expected.drop("sample_name")

    assert_frame_equal(
        comparison.change_counts(
            "age", include_sample_primary_key=include_sample_primary_key
        ),
        expected,
    )


@pytest.mark.parametrize("include_sample_primary_key", [True, False])
def test_change_counts_multiple_primary_key_columns(
    include_sample_primary_key: bool,
) -> None:
    left = pl.DataFrame(
        data={
            "car": ["animal", "car", "plane", "plane"],
            "subcategory": ["yak", "sedan", "jet", "propeller"],
            "weight": [1, 1, 4, 4],
        }
    )
    right = pl.DataFrame(
        data={
            "car": ["animal", "car", "plane"],
            "subcategory": ["yak", "sedan", "jet"],
            "weight": [0, 0, 3],
        }
    )

    comparison = compare_frames(left, right, primary_key=["car", "subcategory"])

    expected = pl.DataFrame(
        data={
            "left": [1, 4],
            "right": [0, 3],
            "count": [2, 1],
            "sample_car": ["animal", "plane"],
            "sample_subcategory": ["yak", "jet"],
        },
        schema={
            "left": pl.Int64,
            "right": pl.Int64,
            "count": pl.UInt32,
            "sample_car": pl.String,
            "sample_subcategory": pl.String,
        },
    )

    if not include_sample_primary_key:
        expected = expected.drop(["sample_car", "sample_subcategory"])

    assert_frame_equal(
        comparison.change_counts(
            "weight", include_sample_primary_key=include_sample_primary_key
        ),
        expected,
    )


def test_change_counts_lazy_parameter() -> None:
    left = pl.DataFrame({"id": ["a", "b"], "value": [1, 2]})
    right = pl.DataFrame({"id": ["a", "b"], "value": [1, None]})
    comparison = compare_frames(left, right, primary_key=["id"])

    result_eager = comparison.change_counts("value")
    assert isinstance(result_eager, pl.DataFrame)

    result_lazy = comparison.change_counts("value", lazy=True)
    assert isinstance(result_lazy, pl.LazyFrame)

    assert_frame_equal(result_eager, result_lazy.collect())
