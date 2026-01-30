# Diffly

A utility package for comparing Polars DataFrames.

```{toctree}
:maxdepth: 2
:hidden:

guides/index
API Reference <api/modules>
```

## What is Diffly?

Diffly is a utility package for comparing Polars DataFrames and LazyFrames with detailed analysis capabilities. It identifies differences between datasets including:

- **Schema differences**: Columns that exist only in one DataFrame
- **Row-level mismatches**: Rows that are different between DataFrames
- **Missing rows**: Rows that exist only in one DataFrame
- **Column value changes**: Detailed analysis of which columns differ and by how much

## Key Features

- **Primary key-based comparison**: Join DataFrames on specified primary keys for row-by-row comparison
- **Tolerance-based equality**: Configure absolute and relative tolerances for floating point comparisons
- **Temporal tolerance**: Support for comparing temporal types (dates, datetimes) with configurable tolerances
- **Rich summaries**: Generate detailed, visually formatted comparison reports
- **Lazy evaluation**: Uses Polars LazyFrames internally for efficient computation
- **Method caching**: Automatically caches comparison results to avoid recomputation
- **Per-column tolerances**: Fine-grained control over comparison tolerances for each column
- **Testing utilities**: Built-in assertion functions for DataFrame and Collection equality in tests

## Quick Example

```python
import polars as pl
from diffly import compare_frames

# Create two DataFrames to compare
left = pl.DataFrame({
    "id": ["a", "b", "c"],
    "value": [1.0, 2.0, 3.0],
    "category": ["x", "y", "z"]
})

right = pl.DataFrame({
    "id": ["a", "b", "d"],
    "value": [1.0, 2.1, 4.0],
    "category": ["x", "y", "w"]
})

# Compare the DataFrames
comparison = compare_frames(left, right, primary_key="id")

# Check if they're equal
if not comparison.equal():
    # Display a detailed summary
    summary = comparison.summary(
        show_perfect_column_matches=True,
        top_k_column_changes=5
    )
    print(summary)
```

## Next Steps

- Follow the [Quickstart Guide](guides/quickstart.md) for a comprehensive introduction
- Explore [Examples](guides/examples/index.md) for common use cases
- Learn about advanced [Features](guides/features/index.md) like tolerances and custom summaries
- Check the [API Reference](api/modules.rst) for detailed function documentation
