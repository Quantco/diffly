# Investigating Differences

When two DataFrames differ, the summary provides an overview. This guide shows how to use `diffly`'s API to drill down into the specific rows and values that differ.

## Investigating column mismatches

{meth}`~diffly.comparison.DataFrameComparison.joined_unequal` returns rows where values differ in the specified columns:

```python
# Get rows where 'amount' differs
mismatched = comparison.joined_unequal("amount")
```

This returns a DataFrame with both the left and right values side-by-side (suffixed `_left` and `_right`), making it easy to compare:

```python
>>> mismatched.select("id", "amount_left", "amount_right")
shape: (42, 3)
┌─────┬─────────────┬──────────────┐
│ id  │ amount_left │ amount_right │
│ --- │ ---         │ ---          │
│ str │ f64         │ f64          │
╞═════╪═════════════╪══════════════╡
│ a1  │ 100.0       │ 100.5        │
│ a2  │ 200.0       │ 200.5        │
│ ... │ ...         │ ...          │
└─────┴─────────────┴──────────────┘
```

These IDs can then be traced through the pipeline to find where the computation diverged.

### Seeing common change patterns

{meth}`~diffly.comparison.DataFrameComparison.change_counts` returns the most frequent value changes for a column:

```python
>>> comparison.change_counts("status")
shape: (3, 3)
┌───────────┬───────────┬───────┐
│ left      │ right     │ count │
│ ---       │ ---       │ ---   │
│ str       │ str       │ u32   │
╞═══════════╪═══════════╪═══════╡
│ pending   │ processed │ 150   │
│ processed │ pending   │ 12    │
│ null      │ pending   │ 3     │
└───────────┴───────────┴───────┘
```

This can reveal systematic issues, like a status transition being applied differently.

## Investigating extra or missing rows

If the summary shows rows in {meth}`~diffly.comparison.DataFrameComparison.left_only` or {meth}`~diffly.comparison.DataFrameComparison.right_only`, there's likely a bug in filtering or join logic.

Retrieve these rows to understand what's different about them:

```python
# Rows in baseline but not in refactored output
missing_rows = comparison.left_only()

# Rows in refactored output but not in baseline
extra_rows = comparison.right_only()
```

```python
>>> comparison.left_only().select("id", "created_at", "source")
shape: (5, 3)
┌─────┬─────────────────────┬────────┐
│ id  │ created_at          │ source │
│ --- │ ---                 │ ---    │
│ str │ datetime[μs]        │ str    │
╞═════╪═════════════════════╪════════╡
│ x1  │ 2024-01-01 00:00:00 │ api    │
│ x2  │ 2024-01-01 00:00:00 │ api    │
│ ... │ ...                 │ ...    │
└─────┴─────────────────────┴────────┘
```

In this example, the missing rows all have `source = "api"` - a clear hint that the refactored code might be filtering out API records incorrectly.

## Working with large DataFrames

All methods shown above accept a `lazy=True` parameter to return a LazyFrame instead of a DataFrame. This can be useful when investigating differences between large DataFrames that don't fit into memory.

```{seealso}
The {class}`~diffly.comparison.DataFrameComparison` API reference for additional methods.
```
