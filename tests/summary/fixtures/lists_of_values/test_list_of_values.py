# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from datetime import date, datetime, timedelta

import polars as pl
import pytest

from diffly import compare_frames
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    ids = [1, 2, 3]

    dates_left = [date(2021, 1, 1) + timedelta(days=i) for i in range(10)]
    dates_right = [d + timedelta(days=1) for d in dates_left]

    datetimes_left = [datetime(2021, 1, 1) + timedelta(days=i) for i in range(10)]
    datetimes_right = [d + timedelta(days=1) for d in datetimes_left]

    ints_left = list(range(10))
    ints_right = [i + 1 for i in ints_left]

    floats_left = [float(i) for i in ints_left]
    floats_right = [float(i + 1) for i in floats_left]

    strings_left = [str(i) for i in ints_left]
    strings_right = [str(i + 1) for i in ints_left]

    bools_left = [True, False] * 5
    bools_right = [not b for b in bools_left]

    left = pl.DataFrame(
        data={
            "id": ids,
            "list_of_dates": [dates_left[:2], dates_left[:5], dates_left],
            "list_of_datetimes": [
                datetimes_left[:2],
                datetimes_left[:5],
                datetimes_left,
            ],
            "list_of_ints": [ints_left[:2], ints_left[:5], ints_left],
            "list_of_floats": [floats_left[:2], floats_left[:5], floats_left],
            "list_of_strings": [strings_left[:2], strings_left[:5], strings_left],
            "list_of_bools": [bools_left[:2], bools_left[:5], bools_left],
        }
    )
    right = pl.DataFrame(
        data={
            "id": ids,
            "list_of_dates": [dates_right[:2], dates_right[:5], dates_right],
            "list_of_datetimes": [
                datetimes_right[:2],
                datetimes_right[:5],
                datetimes_right,
            ],
            "list_of_ints": [ints_right[:2], ints_right[:5], ints_right],
            "list_of_floats": [floats_right[:2], floats_right[:5], floats_right],
            "list_of_strings": [strings_right[:2], strings_right[:5], strings_right],
            "list_of_bools": [bools_right[:2], bools_right[:5], bools_right],
        }
    )
    comp = compare_frames(left, right, primary_key=["id"])
    generate_summaries(comp)
