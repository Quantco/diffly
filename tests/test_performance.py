# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import statistics
import time

import polars as pl

from diffly import compare_frames
from diffly._conditions import condition_equal_columns
from diffly._utils import (
    ABS_TOL_DEFAULT,
    ABS_TOL_TEMPORAL_DEFAULT,
    REL_TOL_DEFAULT,
    Side,
)


def test_summary_lazyframe_not_slower_than_dataframe() -> None:
    """Ensure that passing LazyFrames to summary() doesn't significantly degrade
    performance compared to DataFrames.

    This test verifies that we don't unnecessarily re-collect LazyFrames multiple times
    (e.g., once per column) when computing the summary.
    """
    num_rows = 1_000
    num_columns = 20
    iterations = 50
    num_runs_measured = 10
    num_runs_warmup = 2

    def operation(x: pl.Expr) -> pl.Expr:
        return (x * 31337 + 12345) % 10_000_000_007

    def expensive_computation(col: pl.Expr) -> pl.Expr:
        result = col.cast(pl.Int64)
        for _ in range(iterations):
            result = operation(result)
        return result

    lf = (
        pl.LazyFrame({"idx": range(num_rows)})
        .with_columns(expensive_computation(pl.col("idx")).alias("_shared"))
        .with_columns(
            [operation(pl.col("_shared")).alias(f"col_{i}") for i in range(num_columns)]
        )
        .drop("_shared")
    )
    lf_perturbed = lf.with_columns(
        **{f"col_{i}": pl.col(f"col_{i}") + 1 for i in range(num_columns)}
    )

    times_df = []
    times_lf = []
    for _ in range(num_runs_warmup + num_runs_measured):
        # Benchmark with LazyFrames
        start = time.perf_counter()
        comp_lf = compare_frames(lf, lf_perturbed, primary_key="idx")
        comp_lf.summary(top_k_column_changes=3).format(pretty=False)
        times_lf.append(time.perf_counter() - start)

        # Benchmark with DataFrames (including collection time)
        start = time.perf_counter()
        df = lf.collect()
        df_perturbed = lf_perturbed.collect()
        comp_df = compare_frames(df, df_perturbed, primary_key="idx")
        comp_df.summary(top_k_column_changes=3).format(pretty=False)
        times_df.append(time.perf_counter() - start)

    # Discard the first two runs as warm-up
    mean_time_lf = statistics.mean(times_lf[num_runs_warmup:])
    mean_time_df = statistics.mean(times_df[num_runs_warmup:])

    # LazyFrame should not be significantly slower than DataFrame.
    # Both should do roughly the same work (2 collections + comparison).
    max_allowed_ratio = 1.25
    actual_ratio = mean_time_lf / mean_time_df

    assert actual_ratio < max_allowed_ratio, (
        f"LazyFrame summary took {actual_ratio:.1f}x longer than DataFrame summary "
        f"({mean_time_lf:.3f}s vs {mean_time_df:.3f}s). "
        f"This suggests unnecessary re-collection of LazyFrames."
    )


def test_eq_missing_not_slower_than_element_wise_for_list_columns() -> None:
    """Ensure that comparing list columns with non-tolerance inner types via
    eq_missing() is not slower than the element-wise _compare_sequence_columns()
    path."""
    n_rows = 500_000
    list_len = 20
    num_runs_measured = 10
    num_runs_warmup = 2

    col_left = f"val_{Side.LEFT}"
    col_right = f"val_{Side.RIGHT}"
    df = pl.DataFrame(
        {
            col_left: [list(range(list_len)) for _ in range(n_rows)],
            col_right: [list(range(list_len)) for _ in range(n_rows)],
        }
    )

    times_eq = []
    times_cond = []
    for _ in range(num_runs_warmup + num_runs_measured):
        start = time.perf_counter()
        df.select(pl.col(col_left).eq_missing(pl.col(col_right))).to_series()
        times_eq.append(time.perf_counter() - start)

        start = time.perf_counter()
        df.select(
            condition_equal_columns(
                column="val",
                dtype_left=df.schema[col_left],
                dtype_right=df.schema[col_right],
                max_list_length=list_len,
                abs_tol=ABS_TOL_DEFAULT,
                rel_tol=REL_TOL_DEFAULT,
                abs_tol_temporal=ABS_TOL_TEMPORAL_DEFAULT,
            )
        ).to_series()
        times_cond.append(time.perf_counter() - start)

    mean_time_eq = statistics.mean(times_eq[num_runs_warmup:])
    mean_time_cond = statistics.mean(times_cond[num_runs_warmup:])

    ratio = mean_time_cond / mean_time_eq
    assert ratio < 1.25, (
        f"condition_equal_columns was {ratio:.1f}x slower than eq_missing "
        f"({mean_time_cond:.3f}s vs {mean_time_eq:.3f}s). "
        f"Expected comparable performance since list<i64> should use eq_missing directly."
    )
