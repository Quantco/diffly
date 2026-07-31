# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import datetime as dt
import warnings
from pathlib import Path
from typing import Annotated

import polars as pl

from diffly import compare_frames

from ._compat import typer
from ._utils import ABS_TOL_DEFAULT, ABS_TOL_TEMPORAL_DEFAULT, REL_TOL_DEFAULT
from .metrics.change import DEFAULT_CHANGE_METRICS
from .metrics.data import DEFAULT_DATA_METRICS

app = typer.Typer()


@app.command()
def main(
    left: Annotated[Path, typer.Argument(help="Path to the left parquet file.")],
    right: Annotated[Path, typer.Argument(help="Path to the right parquet file.")],
    primary_key: Annotated[
        list[str],
        typer.Option(
            help=(
                "Primary key columns to use for joining the data frames. If not "
                "provided, comparisons based on joins will raise an error."
            )
        ),
    ] = [],
    abs_tol: Annotated[
        float,
        typer.Option(
            help="Absolute tolerance for numerical comparisons. Default is 1e-08."
        ),
    ] = ABS_TOL_DEFAULT,
    rel_tol: Annotated[
        float,
        typer.Option(
            help="Relative tolerance for numerical comparisons. Default is 1e-05."
        ),
    ] = REL_TOL_DEFAULT,
    abs_tol_temporal: Annotated[
        float,
        typer.Option(
            help=("Absolute tolerance for temporal comparisons. Default is 0 seconds.")
        ),
    ] = ABS_TOL_TEMPORAL_DEFAULT.total_seconds(),
    show_perfect_column_matches: Annotated[
        bool,
        typer.Option(
            help=(
                "Whether to include column matches in the summary even if the column "
                "match rate is 100%. Setting this to ``False`` is useful when comparing "
                "very wide data frames. "
            )
        ),
    ] = True,
    top_k_column_changes: Annotated[
        int,
        typer.Option(
            help=(
                "The maximum number of column values changes to display for columns "
                "with a match rate below 100% in the summary. When enabling this "
                "feature, make sure that no sensitive data is leaked."
            )
        ),
    ] = 0,
    sample_k_rows_only: Annotated[
        int,
        typer.Option(
            help=(
                'The number of rows to show in the Rows "left/right only" '
                "section of the summary. If 0 (default), no rows are shown. Only the "
                "primary key will be printed. An error will be raised if a positive "
                "number is provided and any of the primary key columns is also in "
                "`hidden_columns`. "
            )
        ),
    ] = 0,
    show_sample_primary_key_per_change: Annotated[
        bool,
        typer.Option(
            help=(
                "Whether to show a sample primary key per column change in the summary."
                "If False (default), no primary key values are shown. A sample primary"
                "key can only be shown if `top_k_column_changes` is greater than 0, as"
                "each sample primary key is linked to a specific column change. An "
                "error will be raised if True and any of the primary key columns is also"
                "in `hidden_columns`."
            )
        ),
    ] = False,
    left_name: Annotated[
        str,
        typer.Option(help="Custom display name for the left data frame."),
    ] = "left",
    right_name: Annotated[
        str,
        typer.Option(help="Custom display name for the right data frame."),
    ] = "right",
    slim: Annotated[
        bool,
        typer.Option(
            help=(
                "Whether to generate a slim summary. In slim mode, the summary is as"
                "concise as possible, only showing sections that contain differences."
                "As the structure of the summary can vary, it should only be used by"
                "advanced users who are familiar with the summary format."
            )
        ),
    ] = False,
    output_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help=(
                "Output a machine-readable JSON digest instead of a rich-formatted "
                "summary."
            ),
        ),
    ] = False,
    hidden_column: Annotated[
        list[str],
        typer.Option(
            help=(
                "Columns for which no values are printed, e.g. because they contain"
                "sensitive information."
            )
        ),
    ] = [],
    hidden_columns: Annotated[
        list[str],
        typer.Option(hidden=True),
    ] = [],
    data_metric: Annotated[
        list[str],
        typer.Option(
            help=(
                "Data metric presets to display in the Data Inspection section. "
                f"Repeatable. Available: {', '.join(DEFAULT_DATA_METRICS)}."
            )
        ),
    ] = [],
    change_metric: Annotated[
        list[str],
        typer.Option(
            help=(
                "Change metric presets to display as extra columns in the Columns "
                f"table. Repeatable. Available: {', '.join(DEFAULT_CHANGE_METRICS)}."
            )
        ),
    ] = [],
) -> None:
    """Compare two `parquet` files and print the comparison result."""
    if hidden_columns:
        warnings.warn(
            "`--hidden-columns` is deprecated, use `--hidden-column` instead.",
            FutureWarning,
        )
        hidden_column = [*hidden_column, *hidden_columns]

    for name in data_metric:
        if name not in DEFAULT_DATA_METRICS:
            raise typer.BadParameter(
                f"Unknown data metric: {name!r}. "
                f"Available: {', '.join(DEFAULT_DATA_METRICS)}."
            )
    for name in change_metric:
        if name not in DEFAULT_CHANGE_METRICS:
            raise typer.BadParameter(
                f"Unknown change metric: {name!r}. "
                f"Available: {', '.join(DEFAULT_CHANGE_METRICS)}."
            )
    data_metrics = {name: DEFAULT_DATA_METRICS[name] for name in data_metric}
    change_metrics = {name: DEFAULT_CHANGE_METRICS[name] for name in change_metric}

    comparison = compare_frames(
        pl.scan_parquet(left),
        pl.scan_parquet(right),
        primary_key=None if not primary_key else primary_key,
        abs_tol=abs_tol,
        rel_tol=rel_tol,
        abs_tol_temporal=dt.timedelta(seconds=abs_tol_temporal),
    )
    summary = comparison.summary(
        show_perfect_column_matches=show_perfect_column_matches,
        top_k_column_changes=top_k_column_changes,
        sample_k_rows_only=sample_k_rows_only,
        show_sample_primary_key_per_change=show_sample_primary_key_per_change,
        left_name=left_name,
        right_name=right_name,
        slim=slim,
        hidden_columns=hidden_column,
        data_metrics=data_metrics,
        change_metrics=change_metrics,
    )
    if output_json:
        typer.echo(summary.to_json())
    else:
        typer.echo(summary.format(pretty=True))


if __name__ == "__main__":  # pragma: no cover
    app()
