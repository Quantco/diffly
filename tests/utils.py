# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import inspect
import itertools
import os
import shutil
from pathlib import Path
from typing import Any

import polars as pl

from diffly.comparison import DataFrameComparison


def generate_filename(
    path: Path,
    pretty: bool,
    show_perfect_column_matches: bool,
    show_top_column_changes: bool,
    slim: bool,
    sample_rows: bool,
    sample_pk: bool,
    hidden_columns: bool,
) -> Path:
    with_hidden_columns = "_hidden" if hidden_columns else ""
    return path / (
        f"pretty_{pretty}_perfect_{show_perfect_column_matches}_"
        f"top_{show_top_column_changes}_slim_{slim}_sample_rows_{sample_rows}_sample_pk_{sample_pk}{with_hidden_columns}.txt"
    )


def generate_summaries(
    comparison: DataFrameComparison, outdir: Path | None = None, **summary_kwargs: Any
) -> None:
    if outdir is None:
        current_caller = inspect.stack()[1]
        outdir = Path(current_caller.filename).parent / "gen"
    if os.path.exists(outdir):
        shutil.rmtree(outdir)

    for (
        pretty,
        show_perfect_column_matches,
        show_top_column_changes,
        slim,
        sample_rows,
    ) in itertools.product([True, False], repeat=5):
        sample_pk = sample_rows and show_top_column_changes
        filename = generate_filename(
            outdir,
            pretty,
            show_perfect_column_matches,
            show_top_column_changes,
            slim,
            sample_rows,
            sample_pk,
            False,
        )
        if filename.exists():
            continue

        summary = comparison.summary(
            show_perfect_column_matches=show_perfect_column_matches,
            top_k_column_changes=3 if show_top_column_changes else 0,
            slim=slim,
            sample_k_rows_only=3 if sample_rows else 0,
            show_sample_primary_key_per_change=True if sample_pk else False,
            **summary_kwargs,
        )
        outdir.mkdir(exist_ok=True)
        filename.write_text(summary.format(pretty=pretty))


FRAME_TYPES = [pl.DataFrame, pl.LazyFrame]
TYPING_FRAME_TYPES = type[pl.DataFrame] | type[pl.LazyFrame]
