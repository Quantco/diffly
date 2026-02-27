# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import datetime as dt
import itertools
from collections import defaultdict
from collections.abc import Callable, Mapping

import polars as pl
import pytest
from polars.datatypes.group import (
    FLOAT_DTYPES,
    SIGNED_INTEGER_DTYPES,
    UNSIGNED_INTEGER_DTYPES,
)

from diffly import compare_frames

from .utils import FRAME_TYPES, TYPING_FRAME_TYPES


def test_missing_primary_key_fraction_same() -> None:
    left = pl.DataFrame({"id": ["a", "b", "c"], "value": [1, 2, 3]})
    right = pl.DataFrame({"id": ["a", "b"], "value": [1, 2]})
    comparison = compare_frames(left, right)
    with pytest.raises(ValueError):
        _ = comparison.fraction_same("value")


@pytest.mark.parametrize("frame_type", FRAME_TYPES)
@pytest.mark.parametrize(
    "dtypes_left",
    itertools.zip_longest(
        FLOAT_DTYPES,
        SIGNED_INTEGER_DTYPES,
        UNSIGNED_INTEGER_DTYPES,
        fillvalue=pl.Float32,
    ),
)
@pytest.mark.parametrize(
    "dtypes_right",
    itertools.zip_longest(
        FLOAT_DTYPES,
        SIGNED_INTEGER_DTYPES,
        UNSIGNED_INTEGER_DTYPES,
        fillvalue=pl.Float32,
    ),
)
@pytest.mark.parametrize("parallel", [True, False])
def test_fraction_same(
    frame_type: TYPING_FRAME_TYPES,
    dtypes_left: tuple[pl.DataType, pl.DataType, pl.DataType],
    dtypes_right: tuple[pl.DataType, pl.DataType, pl.DataType],
    parallel: bool,
) -> None:
    left_schema = {
        "name": pl.String,
        "author": pl.String,
        "price": dtypes_left[0],
        "n_copies": dtypes_left[2],
        "n_change_in_stock": dtypes_left[1],
        "publishing_date": pl.Date,
        "metadata": pl.Struct({"genre": pl.String, "n_pages": dtypes_left[2]}),
        "ratings": pl.List(dtypes_left[0]),
        "is_fiction": pl.Boolean,
        "v_left": dtypes_left[0],
    }
    right_schema = {
        "name": pl.String,
        "author": pl.String,
        "price": dtypes_right[0],
        "n_copies": dtypes_right[2],
        "n_change_in_stock": dtypes_right[1],
        "publishing_date": pl.Date,
        "metadata": pl.Struct({"genre": pl.String, "n_pages": dtypes_right[2]}),
        "ratings": pl.List(dtypes_right[0]),
        "is_fiction": pl.Boolean,
        "v_right": dtypes_right[0],
    }
    left = frame_type(
        data={
            "name": ["Book A", "Book B", "Book C", "Book D"],
            "author": ["Author 1", "Author 2", "Author 3", "Author 4"],
            "price": [10.0, 15.0, 20.0, 25.0],
            "n_copies": [5, 3, 10, 2],
            "n_change_in_stock": [1, -5, 0, 2],
            "publishing_date": [
                dt.date(2023, 1, 1),
                dt.date(2023, 1, 2),
                dt.date(2023, 1, 3),
                dt.date(2023, 1, 4),
            ],
            "metadata": [
                {"genre": "Fiction", "n_pages": 30},
                {"genre": "Non-Fiction", "n_pages": 20},
                {"genre": "Fiction", "n_pages": 40},
                {"genre": "Non-Fiction", "n_pages": 10},
            ],
            "ratings": [
                [4.5, 4.0, 5.0],
                [3.0, 4.0, 2.5],
                [5.0, 4.5],
                [3.5, 4.0],
            ],
            "is_fiction": [True, False, True, False],
            "v_left": [1.0, 2.0, 3.0, 4.0],
        },
        schema=left_schema,  # type: ignore
    )
    right = frame_type(
        data={
            "name": ["Book A", "Book B", "Book C", "Book E"],
            "author": ["Author 1", "Author 2", "Author 3", "Author 4"],
            "price": [10.0, 15.0, 19.99, 25.0],
            "n_copies": [6, 29, 10, 2],
            "n_change_in_stock": [0, 7, -9, 2],
            "publishing_date": [
                dt.date(2023, 1, 1),
                dt.date(2025, 1, 2),
                dt.date(2023, 1, 3),
                dt.date(2023, 1, 4),
            ],
            "metadata": [
                {"genre": "Romance", "n_pages": 30},
                {"genre": "Non-Fiction", "n_pages": 21},
                {"genre": "Fiction", "n_pages": 40},
                {"genre": "Non-Fiction", "n_pages": 15},
            ],
            "ratings": [
                [4.5, 4.0, 5.0],
                [3.0, 4.2, 2.5],
                [5.0, 4.5],
                [3.5, 4.0],
            ],
            "is_fiction": [True, False, True, False],
            "v_right": [1.0, 2.0, 2.0, 2.0],
        },
        schema=right_schema,  # type: ignore
    )

    comparison = compare_frames(left, right, primary_key=["name"])
    expected = {
        "author": 3 / 3,
        "price": 2 / 3,
        "n_copies": 1 / 3,
        "n_change_in_stock": 0 / 3,
        "publishing_date": 2 / 3,
        "metadata": 1 / 3,
        "ratings": 2 / 3,
        "is_fiction": 3 / 3,
    }
    if parallel:
        actual = comparison.fraction_same()
    else:
        actual = {col: comparison.fraction_same(col) for col in expected.keys()}
    assert actual == expected


def test_fraction_same_join_column() -> None:
    left = pl.DataFrame({"id": ["a", "b"], "value": [1, 2]})
    right = pl.DataFrame({"id": ["a", "b"], "value": [1, 3]})
    primary_key = "id"
    comparison = compare_frames(left, right, primary_key=primary_key)
    with pytest.raises(
        ValueError,
        match="is a join column for which a fraction of matching values cannot be computed.",
    ):
        comparison.fraction_same(primary_key)


def test_fraction_same_no_non_join_columns() -> None:
    df = pl.DataFrame({"id": ["a", "b", "c"]})
    comparison = compare_frames(df, df, primary_key=["id"])
    assert comparison.fraction_same() == dict()


def test_fraction_same_uncommon_column() -> None:
    left = pl.DataFrame({"id": ["a", "b"], "value": [1, 2]})
    right = pl.DataFrame({"id": ["a", "b"], "other": [3, 4]})
    primary_key = ["id"]
    comparison = compare_frames(left, right, primary_key=primary_key)
    with pytest.raises(
        ValueError,
        match="is not a common column, so the fraction of matching values cannot be computed.",
    ):
        comparison.fraction_same("nonexistent_column")


@pytest.mark.parametrize(
    ("perturbation", "abs_tol", "rel_tol"),
    [
        # NOTE: Double perturbations for tolerations to prevent odd floating point
        #  comparison issues
        (lambda col: col + 0.01, 0.02, 1e-05),
        (lambda col: col * 1.01, 1e-08, 0.02),
    ],
)
def test_float_tolerance_fraction_same(
    perturbation: Callable[[pl.Expr], pl.Expr],
    abs_tol: float,
    rel_tol: float,
) -> None:
    base = pl.DataFrame(
        {"id": ["a", "b", "c"], "x": [1.0, None, 3.0], "y": [4.0, 5.0, 6.0]}
    )
    noisy = base.with_columns(x=None, y=perturbation(pl.col("y")))
    primary_key = ["id"]

    comparison_default = compare_frames(base, noisy, primary_key=primary_key)
    assert comparison_default.fraction_same("x") == 1 / 3
    assert comparison_default.fraction_same("y") == 0 / 3

    comparison_lenient = compare_frames(
        base,
        noisy,
        primary_key=primary_key,
        abs_tol=abs_tol,
        rel_tol=rel_tol,
    )
    assert comparison_lenient.fraction_same("y") == 1.0


@pytest.mark.parametrize(
    "abs_tol",
    [
        defaultdict(lambda: 0.01, {"value2": 0.2}),
        {"value": 0.01, "value2": 0.2},
    ],
    ids=["defaultdict", "dict"],
)
def test_float_abs_tol_mapping_fraction_same(abs_tol: Mapping[str, float]) -> None:
    df = pl.DataFrame(
        data=[
            ("a", 2.0, 2.0),
            ("b", 3.0, 3.0),
            ("c", 4.0, 4.0),
        ],
        schema={
            "id": pl.String,
            "value": pl.Float64,
            "value2": pl.Float64,
        },
        orient="row",
    )
    df_pertubed = df.with_columns(pl.col("value") + 0.01, pl.col("value2") + 0.19)

    comparison = compare_frames(df, df_pertubed, primary_key="id")
    assert comparison.fraction_same("value") == 0.0
    assert comparison.fraction_same("value2") == 0.0

    comparison_lenient = compare_frames(
        df,
        df_pertubed,
        primary_key="id",
        abs_tol=abs_tol,
    )
    assert comparison_lenient.fraction_same("value") == 1.0
    assert comparison_lenient.fraction_same("value2") == 1.0


@pytest.mark.parametrize(
    "rtol",
    [
        defaultdict(lambda: 0.01, {"value2": 0.2}),
        {"value": 0.01, "value2": 0.2},
    ],
    ids=["defaultdict", "dict"],
)
def test_float_rtol_mapping_fraction_same(rtol: Mapping[str, float]) -> None:
    df = pl.DataFrame(
        data=[
            ("a", 2.0, 2.0),
            ("b", 3.0, 3.0),
            ("c", 4.0, 4.0),
        ],
        schema={
            "id": pl.String,
            "value": pl.Float64,
            "value2": pl.Float64,
        },
        orient="row",
    )
    df_pertubed = df.with_columns(pl.col("value") * 1.01, pl.col("value2") * 1.19)

    comparison = compare_frames(df, df_pertubed, primary_key="id")
    assert comparison.fraction_same("value") == 0.0
    assert comparison.fraction_same("value2") == 0.0

    comparison_lenient = compare_frames(
        df,
        df_pertubed,
        primary_key="id",
        rel_tol=rtol,
    )
    assert comparison_lenient.fraction_same("value") == 1.0
    assert comparison_lenient.fraction_same("value2") == 1.0


@pytest.mark.parametrize(
    "abs_tol_temporal",
    [
        defaultdict(
            lambda: dt.timedelta(seconds=1), {"value2": dt.timedelta(seconds=2)}
        ),
        {"value": dt.timedelta(seconds=1), "value2": dt.timedelta(seconds=2)},
    ],
    ids=["defaultdict", "dict"],
)
def test_abs_tol_temporal_mapping_fraction_same(
    abs_tol_temporal: Mapping[str, dt.timedelta],
) -> None:
    df = pl.DataFrame(
        data=[
            ("a", dt.datetime(2025, 1, 1, 0, 0, 0), dt.datetime(2025, 1, 1, 0, 0, 0)),
            ("b", dt.datetime(2025, 1, 2, 0, 0, 0), dt.datetime(2025, 1, 2, 0, 0, 0)),
            ("c", dt.datetime(2025, 1, 3, 0, 0, 0), dt.datetime(2025, 1, 3, 0, 0, 0)),
        ],
        schema={
            "id": pl.String,
            "value": pl.Datetime,
            "value2": pl.Datetime,
        },
        orient="row",
    )
    df_pertubed = df.with_columns(
        pl.col("value").dt.offset_by("1s"), pl.col("value2").dt.offset_by("2s")
    )

    comparison = compare_frames(df, df_pertubed, primary_key="id")
    assert comparison.fraction_same("value") == 0.0
    assert comparison.fraction_same("value2") == 0.0

    comparison_lenient = compare_frames(
        df,
        df_pertubed,
        primary_key="id",
        abs_tol_temporal=abs_tol_temporal,
    )
    assert comparison_lenient.fraction_same("value") == 1.0
    assert comparison_lenient.fraction_same("value2") == 1.0
