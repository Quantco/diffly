# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import pytest

import diffly

FirstEnum = pl.Enum(["one", "two"])
SecondEnum = pl.Enum(["one", "two", "three"])


@pytest.mark.parametrize(
    "first_type,second_type",
    [
        # Test all combinations where second type is not FirstEnum (not valid)
        (FirstEnum, SecondEnum),
        (SecondEnum, SecondEnum),
        (FirstEnum, pl.Categorical),
        (SecondEnum, pl.Categorical),
        (pl.Categorical, SecondEnum),
        (pl.Categorical, pl.Categorical),
    ],
)
def test_enum_and_categorical_comparison_mismatch(
    first_type: type[pl.Enum | pl.Categorical],
    second_type: type[pl.Enum | pl.Categorical],
) -> None:
    # Arrange
    df1 = pl.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["one", "two", "one"],
        },
        schema_overrides={"b": first_type},
    )
    df2 = pl.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["one", "two", "three"],
        },
        schema_overrides={"b": second_type},
    )

    # Act
    unequal = diffly.compare_frames(df1, df2, primary_key=["a"]).joined_unequal()

    # Assert
    assert len(unequal) == 1
    assert unequal.select("a").item() == 3


@pytest.mark.parametrize("first_type", [FirstEnum, SecondEnum, pl.Categorical])
@pytest.mark.parametrize("second_type", [FirstEnum, SecondEnum, pl.Categorical])
@pytest.mark.parametrize("check_dtypes", [True, False])
def test_enum_and_categorical_comparison_equal(
    first_type: type[pl.Enum | pl.Categorical],
    second_type: type[pl.Enum | pl.Categorical],
    check_dtypes: bool,
) -> None:
    # Arrange
    df1 = pl.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["one", "two", "one"],
        },
        schema_overrides={"b": first_type},
    )
    df2 = pl.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["one", "two", "one"],
        },
        schema_overrides={"b": second_type},
    )

    # Act and assert
    # The data frames are equal if we do not check dtypes or if the types are the same.
    expected = not check_dtypes or first_type == second_type
    assert (
        diffly.compare_frames(df1, df2, primary_key=["a"]).equal(
            check_dtypes=check_dtypes
        )
        == expected
    )
