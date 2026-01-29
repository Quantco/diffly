# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from pathlib import Path

import polars as pl
import pytest

from diffly import compare_frames
from tests.utils import generate_summaries


@pytest.mark.generate
def test_generate() -> None:
    num_rows = int(1e6)
    long_columns_left = [
        ("z_multidimensional_data_visualization_technique_optimization_parameter")
    ]
    long_columns_right = [
        ("pharmacovigilance_adverse_event_reporting_compliance_documentation_status")
    ]
    long_columns_common = [
        (
            "international_regulatory_compliance_documentation_submission_"
            "tracking_system_integration_connection_parameter_configuration_setting_"
            "identifier"
        )
    ]
    short_columns_left = ["fizz", "buzz", "waldo"]
    short_columns_right = ["bing", "bong", "zork"]
    short_columns_common = ["qux", "quux"]
    short_columns_common_mm = ["quuux"]

    left = pl.DataFrame(
        {
            "key": range(num_rows),
            "value": [0] * num_rows,
            **{col_name: [0] * num_rows for col_name in long_columns_left},
            **{col_name: [0] * num_rows for col_name in long_columns_common},
            **{col_name: [0] * num_rows for col_name in short_columns_left},
            **{col_name: [0] * num_rows for col_name in short_columns_common},
            **{col_name: [0] * num_rows for col_name in short_columns_common_mm},
        }
    )
    right = pl.DataFrame(
        {
            "key": range(num_rows),
            "value": [1] * (num_rows // 2) + [0] * (num_rows // 2),
            **{col_name: [0] * num_rows for col_name in long_columns_right},
            **{col_name: [0] * num_rows for col_name in long_columns_common},
            **{col_name: [0] * num_rows for col_name in short_columns_right},
            **{col_name: [0] * num_rows for col_name in short_columns_common},
            **{col_name: [1] * num_rows for col_name in short_columns_common_mm},
        }
    )
    comparison = compare_frames(left, right, primary_key=["key"])

    generate_summaries(comparison, outdir=Path(__file__).parent / "gen")
