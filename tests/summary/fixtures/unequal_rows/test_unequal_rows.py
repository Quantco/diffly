# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from pathlib import Path

import polars as pl
import pytest

from diffly import compare_frames
from tests.utils import generate_filename, generate_summaries


@pytest.mark.generate
@pytest.mark.parametrize("pretty", [True, False])
def test_generate(pretty: bool) -> None:
    left = pl.DataFrame(
        data={
            "name": ["tiger", "sloth", "elephant", "ant", "dolphin", "parrot"],
            "weight_kg": [200.0, 5.0, None, 0.00001, 50.0, 1.5],
            "speed_kph": [None, 0.27, 40.0, None, 14.5, 56.0],
            "life_expectancy": [11, 11, 48, 1, 12, 23],
        }
    )
    right = left.with_columns(pl.col("life_expectancy").replace({11: 12, 12: 35}))
    comp = compare_frames(left, right, primary_key=["name"])
    generate_summaries(comp)

    summary = comp.summary(
        show_perfect_column_matches=False,
        sample_k_rows_only=3,
        hidden_columns=["life_expectancy"],
    )

    outdir = Path(__file__).parent / "gen"
    outdir.mkdir(exist_ok=True)
    generate_filename(
        path=outdir,
        pretty=pretty,
        show_perfect_column_matches=False,
        show_top_column_changes=True,
        slim=False,
        sample_rows=True,
        sample_pk=False,
        hidden_columns=True,
    ).write_text(summary.format(pretty=pretty))
