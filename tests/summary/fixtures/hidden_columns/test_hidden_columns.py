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
        {
            "id": [1, 2, 3, 4],
            "amount": [10.0, 20.0, 30.0, 40.0],
            "card_id": ["a", "b", "c", "d"],
            "status": ["x", "y", "z", "w"],
        }
    )
    right = pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "amount": [10.0, 25.0, 30.0, 45.0],
            "card_id": ["a", "B", "c", "D"],
            "status": ["x", "y", "z", "w"],
        }
    )
    comp = compare_frames(left, right, primary_key=["id"])
    generate_summaries(comp)

    summary = comp.summary(
        top_k_column_changes=3,
        hidden_columns=["card_id"],
    )

    outdir = Path(__file__).parent / "gen"
    outdir.mkdir(exist_ok=True)
    for p in [True, False]:
        generate_filename(
            path=outdir,
            pretty=p,
            show_perfect_column_matches=True,
            show_top_column_changes=True,
            slim=False,
            sample_rows=False,
            sample_pk=False,
            hidden_columns=True,
        ).write_text(summary.format(pretty=p))
