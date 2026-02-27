# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from collections.abc import Sequence

import polars as pl
import pytest

from diffly import compare_frames


@pytest.mark.parametrize("primary_key", ["name", ["name"], ("name")])
def test_primary_key_sequence_types(primary_key: str | Sequence[str]) -> None:
    left = pl.DataFrame({"name": ["a", "b"], "value": [1, 2]})
    right = pl.DataFrame({"name": ["a", "b"], "other": [3, 4]})
    comparison = compare_frames(left, right, primary_key=primary_key)
    assert comparison.primary_key == ["name"]


def test_empty_primary_key() -> None:
    left = pl.DataFrame({"name": ["a", "b"], "value": [1, 2]})
    right = pl.DataFrame({"name": ["a", "b"], "other": [3, 4]})
    with pytest.raises(ValueError, match="empty"):
        compare_frames(left, right, primary_key=[])


def test_missing_primary_key() -> None:
    left = pl.DataFrame({"name": ["a", "b"], "value": [1, 2]})
    right = pl.DataFrame({"name": ["a", "b"], "other": [3, 4]})
    # Primary key that neither frame has
    with pytest.raises(ValueError, match="left.*missing.*co2_emissions"):
        compare_frames(left, right, primary_key=["co2_emissions"])
    # Primary key that the right frame does not have
    with pytest.raises(ValueError, match="right.*missing.*value"):
        compare_frames(left, right, primary_key=["value"])


def test_pk_violation() -> None:
    df_valid = pl.DataFrame({"id": ["a", "b"], "value": [1, 2]})
    df_duplicates = pl.DataFrame({"id": ["a", "a"], "value": [1, 2]})
    with pytest.raises(ValueError, match="primary key.*left"):
        compare_frames(df_duplicates, df_valid, primary_key=["id"])
    with pytest.raises(ValueError, match="primary key.*right"):
        compare_frames(df_valid, df_duplicates, primary_key=["id"])


def test_incompatible_primary_key_dtypes() -> None:
    with pytest.warns(UserWarning, match=".*datatypes of join keys don't match.*"):
        comparison = compare_frames(
            pl.DataFrame({"key": ["tiger"], "speed_kph": [5.0]}),
            pl.DataFrame({"key": [1], "speed_kph": [5.0]}),
            primary_key=["key"],
        )
    comparison.summary()


def test_incomplete_mapping() -> None:
    with pytest.raises(
        KeyError,
        match="The mapping needs to specify a value for every common column except "
        "the primary key.",
    ):
        compare_frames(
            pl.DataFrame({"key": ["tiger"], "speed_kph": [5.0], "weight_kg": [200.0]}),
            pl.DataFrame({"key": ["tiger"], "speed_kph": [5.0], "weight_kg": [200.0]}),
            primary_key=["key"],
            abs_tol={"speed_kph": 0.1},
        )


def test_overspecified_mapping() -> None:
    with pytest.raises(
        KeyError,
        match="The mapping must only contain common columns except the primary key. "
        "However, it also contains the following columns: {'weight_kg'}.",
    ):
        compare_frames(
            pl.DataFrame({"key": ["tiger"], "speed_kph": [5.0]}),
            pl.DataFrame({"key": ["tiger"], "speed_kph": [5.0]}),
            primary_key=["key"],
            abs_tol={"speed_kph": 0.1, "weight_kg": 10.0},
        )
