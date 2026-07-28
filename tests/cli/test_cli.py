# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from pathlib import Path

import polars as pl
import pytest

pytest.importorskip("typer", reason="requires typer")

from typer.testing import CliRunner

from diffly import compare_frames
from diffly.cli import app

runner = CliRunner()


@pytest.mark.parametrize("output_json", [False, True])
def test_cli_smoke(tmp_path: Path, output_json: bool) -> None:
    left = pl.DataFrame(
        {
            "name": ["cat", "dog", "mouse"],
            "weight_kg": [5.0, 10.0, 0.05],
            "age": [3, 5, 1],
        }
    )
    right = pl.DataFrame(
        {
            "name": ["cat", "dog", "mouse"],
            "weight_kg": [5.5, 10.0, 0.05],
            "age": [3, 5, 2],
            "color": ["orange", "brown", "gray"],
        }
    )

    left.write_parquet(tmp_path / "left.parquet")
    right.write_parquet(tmp_path / "right.parquet")
    args = [
        str(tmp_path / "left.parquet"),
        str(tmp_path / "right.parquet"),
        "--primary-key",
        "name",
    ]
    if output_json:
        args.append("--json")
    result = runner.invoke(app, args, color=True)
    comparison = compare_frames(
        pl.scan_parquet(tmp_path / "left.parquet"),
        pl.scan_parquet(tmp_path / "right.parquet"),
        primary_key="name",
    )
    assert result.exit_code == 0

    if output_json:
        assert result.output == comparison.summary().to_json() + "\n"
    else:
        assert result.output == comparison.summary().format(pretty=True) + "\n"


def test_cli_hidden_columns_alias_warns(tmp_path: Path) -> None:
    df = pl.DataFrame({"id": [1, 2], "secret": ["a", "b"]})
    df.write_parquet(tmp_path / "left.parquet")
    df.write_parquet(tmp_path / "right.parquet")

    with pytest.warns(FutureWarning, match="--hidden-columns.*deprecated"):
        result = runner.invoke(
            app,
            [
                str(tmp_path / "left.parquet"),
                str(tmp_path / "right.parquet"),
                "--primary-key",
                "id",
                "--hidden-columns",
                "secret",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 0


@pytest.mark.parametrize(
    "flag, name, expected",
    [
        ("--change-metric", "bogus", "Unknown change metric"),
        ("--data-metric", "bogus", "Unknown data metric"),
        # A valid preset from the wrong family is rejected by the family-specific flag.
        ("--change-metric", "Null%", "Unknown change metric"),
        ("--data-metric", "Mean diff", "Unknown data metric"),
    ],
)
def test_cli_unknown_metric(
    tmp_path: Path, flag: str, name: str, expected: str
) -> None:
    left = pl.DataFrame({"id": [1, 2], "x": [1.0, 2.0]})
    right = pl.DataFrame({"id": [1, 2], "x": [1.0, 3.0]})
    left.write_parquet(tmp_path / "left.parquet")
    right.write_parquet(tmp_path / "right.parquet")

    result = runner.invoke(
        app,
        [
            str(tmp_path / "left.parquet"),
            str(tmp_path / "right.parquet"),
            "--primary-key",
            "id",
            flag,
            name,
        ],
    )
    assert result.exit_code != 0
    assert expected in result.output


@pytest.mark.parametrize(
    "flag, metric_name", [("--change-metric", "Mean diff"), ("--data-metric", "Null%")]
)
def test_cli_metric_from_both_defaults(
    tmp_path: Path, flag: str, metric_name: str
) -> None:
    # Change presets are selectable via --change-metric, data presets via --data-metric.
    left = pl.DataFrame({"id": [1, 2], "x": [1.0, None]})
    right = pl.DataFrame({"id": [1, 2], "x": [1.0, 3.0]})
    left.write_parquet(tmp_path / "left.parquet")
    right.write_parquet(tmp_path / "right.parquet")

    result = runner.invoke(
        app,
        [
            str(tmp_path / "left.parquet"),
            str(tmp_path / "right.parquet"),
            "--primary-key",
            "id",
            flag,
            metric_name,
        ],
    )
    assert result.exit_code == 0
    assert metric_name in result.output
