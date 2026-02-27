# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import pytest
from pytest_lazy_fixtures import lf

from diffly import compare_frames


@pytest.fixture()
def df_full() -> pl.DataFrame:
    return pl.DataFrame({"id": ["a", "b", "c"], "value": [1, 2, 3]})


@pytest.fixture()
def df_subset(df_full: pl.DataFrame) -> pl.DataFrame:
    return df_full.slice(0, 2)


@pytest.mark.parametrize(
    ("left", "right", "result"),
    [
        (lf("df_full"), lf("df_full"), True),
        (lf("df_full"), lf("df_subset"), False),
    ],
)
def test_equal_num_rows(left: pl.DataFrame, right: pl.DataFrame, result: bool) -> None:
    assert compare_frames(left, right).equal_num_rows() == result
