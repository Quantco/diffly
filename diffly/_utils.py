# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import datetime as dt
from collections.abc import Mapping
from enum import StrEnum
from typing import TypeVar

import polars as pl


def lazy_len(lf: pl.LazyFrame) -> int:
    return lf.select(pl.len()).collect().item()


def is_primary_key(lf: pl.LazyFrame, columns: list[str]) -> bool:
    return not lf.select(pl.struct(*columns).is_duplicated().any()).collect().item()


def get_select_columns(keep: list[str], expand: list[str]) -> list[str]:
    return keep + sum(
        [[f"{col}_{Side.LEFT}", f"{col}_{Side.RIGHT}"] for col in expand], []
    )


T = TypeVar("T", float, dt.timedelta, int)


def make_and_validate_mapping(
    value_or_mapping: T | Mapping[str, T], other_common_columns: list[str]
) -> dict[str, T]:
    if isinstance(value_or_mapping, Mapping):
        for col in other_common_columns:
            try:
                value_or_mapping[col]
            except KeyError:
                raise KeyError(
                    "The mapping needs to specify a value for every common column except "
                    "the primary key."
                )
        if diff := (set(value_or_mapping.keys()) - set(other_common_columns)):
            raise KeyError(
                f"The mapping must only contain common columns except the primary key. "
                f"However, it also contains the following columns: {diff}."
            )
        return {col: value_or_mapping[col] for col in other_common_columns}
    return {col: value_or_mapping for col in other_common_columns}


def capitalize_first(s: str) -> str:
    return s[0].upper() + s[1:] if s else s


ABS_TOL_DEFAULT = 1e-08
REL_TOL_DEFAULT = 1e-05
ABS_TOL_TEMPORAL_DEFAULT = dt.timedelta(0)


class Side(StrEnum):
    "Side refers to either the left or right dataframe in a comparison."

    LEFT = "left"
    RIGHT = "right"
