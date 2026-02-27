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


def test_cli_smoke(tmp_path: Path) -> None:
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
    result = runner.invoke(
        app,
        [
            str(tmp_path / "left.parquet"),
            str(tmp_path / "right.parquet"),
            "--primary-key",
            "name",
        ],
        color=True,
    )
    comparison = compare_frames(
        pl.scan_parquet(tmp_path / "left.parquet"),
        pl.scan_parquet(tmp_path / "right.parquet"),
        primary_key="name",
    )
    assert result.exit_code == 0
    assert result.output == comparison.summary().format(pretty=True) + "\n"
