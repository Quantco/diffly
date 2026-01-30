# Quickstart

For the purpose of this guide, let's assume we're working with sales data from two different time periods or systems, and we want to identify what has changed between them. As a running example, consider the following data sets:

**Left (Baseline)**:
| `product_id` | `name` | `price` | `stock` | `category` |
|--------------|--------|---------|---------|------------|
| "A001" | "Widget" | 19.99 | 100 | "Tools" |
| "A002" | "Gadget" | 29.99 | 50 | "Electronics" |
| "A003" | "Doohickey" | 9.99 | 200 | "Tools" |

**Right (Current)**:
| `product_id` | `name` | `price` | `stock` | `category` |
|--------------|--------|---------|---------|------------|
| "A001" | "Widget" | 21.99 | 95 | "Tools" |
| "A002" | "Gadget" | 29.99 | 50 | "Electronics" |
| "A004" | "Thingamajig" | 14.99 | 75 | "Hardware" |

## Basic Comparison

To get started with diffly, you'll use the {func}`~diffly.compare_frames` function to compare two Polars DataFrames:

```python
import polars as pl
from diffly import compare_frames

left = pl.DataFrame({
    "product_id": ["A001", "A002", "A003"],
    "name": ["Widget", "Gadget", "Doohickey"],
    "price": [19.99, 29.99, 9.99],
    "stock": [100, 50, 200],
    "category": ["Tools", "Electronics", "Tools"]
})

right = pl.DataFrame({
    "product_id": ["A001", "A002", "A004"],
    "name": ["Widget", "Gadget", "Thingamajig"],
    "price": [21.99, 29.99, 14.99],
    "stock": [95, 50, 75],
    "category": ["Tools", "Electronics", "Hardware"]
})

# Compare the data frames
comparison = compare_frames(left, right, primary_key="product_id")
```

This creates a {class}`~diffly.DataFrameComparison` object that can be used to explore the differences between the two data frames.

## Checking Equality

The simplest check is whether the two data frames are equal:

```python
if comparison.equal():
    print("Data frames are identical!")
else:
    print("Data frames have differences")
```

In our example, this will print "Data frames have differences" because:

- Product A001's price changed from 19.99 to 21.99
- Product A001's stock changed from 100 to 95
- Product A003 exists only in the left data frame
- Product A004 exists only in the right data frame

## Generating a Summary

To understand what changed, you can generate a detailed summary:

```python
summary = comparison.summary()
print(summary)
```

This will display a rich, formatted summary showing:

- **Schema differences**: Columns that exist only in one data frame
- **Row differences**: Rows that exist only in one data frame (based on the primary key)
- **Column match rates**: What percentage of rows have matching values for each column

The summary provides a high-level overview of all differences in a human-readable format.

## Customizing the Summary

You can customize what the summary displays:

```python
summary = comparison.summary(
    show_perfect_column_matches=True,  # Show columns with 100% match rate
    top_k_column_changes=5,  # Show up to 5 example value changes per column
    sample_k_rows_only=3,  # Show 3 sample rows that exist only on left/right
    show_sample_primary_key_per_change=True,  # Show which row changed for each example
    left_name="Baseline",  # Custom name for left data frame
    right_name="Current"  # Custom name for right data frame
)
print(summary)
```

This enhanced summary will include:

- Actual example values that changed (e.g., "19.99 → 21.99")
- Sample primary keys for rows that exist only on one side
- Custom labels for the two data frames being compared

```{note}
When using `top_k_column_changes` or `sample_k_rows_only`, make sure that no sensitive data is leaked, as actual data values will be displayed in the summary.
```

## Configuring Tolerances

When comparing floating point numbers or temporal data, exact equality may not be appropriate. diffly supports tolerance-based comparisons:

```python
import datetime as dt

comparison = compare_frames(
    left,
    right,
    primary_key="product_id",
    abs_tol=0.01,  # Absolute tolerance for floating point columns
    rel_tol=0.001,  # Relative tolerance (0.1%) for floating point columns
    abs_tol_temporal=dt.timedelta(seconds=1)  # Tolerance for datetime columns
)
```

With `abs_tol=0.01`, prices that differ by less than $0.01 will be considered equal.

### Per-Column Tolerances

For fine-grained control, you can specify different tolerances for different columns:

```python
comparison = compare_frames(
    left,
    right,
    primary_key="product_id",
    abs_tol={"price": 0.50, "stock": 5.0},  # Allow $0.50 price variance, 5 unit stock variance
    rel_tol={"price": 0.01}  # Allow 1% relative difference in price
)
```

This allows `price` and `stock` to have different comparison criteria while other numeric columns use the default tolerances.

## Working Without a Primary Key

If your data frames don't have a natural primary key, you can still perform comparisons:

```python
comparison = compare_frames(left, right)  # No primary key specified

# Check if schemas match
if comparison.equal():
    print("Data frames are identical")
```

Without a primary key, diffly can only check:

- Schema equality (column names and types)
- Overall equality (including row order)

Row-level comparisons and detailed summaries require a primary key to join the data frames.

## Comparing LazyFrames

diffly works seamlessly with both {class}`polars.DataFrame` and {class}`polars.LazyFrame`:

```python
left_lazy = pl.scan_parquet("baseline.parquet")
right_lazy = pl.scan_parquet("current.parquet")

comparison = compare_frames(left_lazy, right_lazy, primary_key="product_id")

# Computation is deferred until you call methods like equal() or summary()
if not comparison.equal():
    print(comparison.summary())
```

Internally, diffly uses lazy evaluation for efficiency, only computing what's necessary when you request specific comparisons.

## Ignoring Data Type Differences

Sometimes you want to ignore differences in data types (e.g., `Int32` vs `Int64`):

```python
# This will be False if dtypes differ
comparison.equal()

# This will be True if values match, regardless of dtype
comparison.equal(check_dtypes=False)
```

## Using diffly in Tests

diffly provides assertion functions for use in test suites:

```python
from diffly import assert_frame_equal

def test_data_transformation():
    result = transform_data(input_df)
    expected = pl.DataFrame(...)

    # Will raise AssertionError with detailed summary if frames don't match
    assert_frame_equal(
        result,
        expected,
        primary_key="id",
        abs_tol=1e-6
    )
```

For comparing dataframely collections:

```python
from diffly import assert_collection_equal

def test_pipeline_output():
    result = run_pipeline(input_data)
    expected = load_expected_output()

    assert_collection_equal(
        result,
        expected,
        abs_tol=1e-6,
        top_k_column_changes=10  # Show examples if assertion fails
    )
```

## Outlook

This concludes the quickstart guide. For more information, please see the [API Reference](../api/modules.rst) to explore all available comparison methods and configuration options.
