# Diffly

A utility package for comparing `polars` data frames.

```{toctree}
:maxdepth: 2
:hidden:

guides/index
API Reference <api/index>
```

## What is Diffly?

Diffly is a utility package for comparing `polars` data frames and lazy frames with detailed analysis capabilities. It identifies differences between datasets including:

- **Schema differences**: Columns that exist only in one data frame
- **Row-level mismatches**: Rows that are different between data frames
- **Missing rows**: Rows that exist only in one data frame
- **Column value changes**: Detailed analysis of which columns differ and by how much

## Key Features

- **Primary key-based comparison**: Join data frames on specified primary keys for row-by-row comparison
- **Rich summaries**: Generate detailed, visually formatted comparison reports
- **Tolerance-based equality**: Configure absolute and relative tolerances for floating point comparisons
- **Lazy evaluation**: Uses `polars` lazy frames internally for efficient computation
- **Temporal tolerance**: Support for comparing temporal types (dates, datetimes) with configurable tolerances
- **Per-column tolerances**: Fine-grained control over comparison tolerances for each column
- **Method caching**: Automatically caches comparison results to avoid recomputation
- **Testing utilities**: Built-in assertion functions for data frame and collection equality in tests

## Quick Example

```python
import polars as pl
from diffly import compare_frames

# Create two data frames to compare
left = pl.DataFrame({
    "id": ["a", "b", "c"],
    "value": [1.0, 2.0, 3.0],
})

right = pl.DataFrame({
    "id": ["a", "b", "d"],
    "value": [1.0, 2.5, 4.0],
})

# Compare the data frames
comparison = compare_frames(left, right, primary_key="id")

# Check if they're equal
if not comparison.equal():
    # Display a detailed summary
    summary = comparison.summary(
        top_k_column_changes=1,
        show_sample_primary_key_per_change=True,
    )
    print(summary)
```

This prints a rich summary showing schema differences, row counts, match rates, and top value changes:

```
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃                                     Diffly Summary                                     ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
   Primary key: id

 Schemas
 ▔▔▔▔▔▔▔
   Schemas match exactly (column count: 2).

 Rows
 ▔▔▔▔
   Left count             Right count
       3      (no change)      3

   ┏━┯━┯━┯━┯━┓
   ┃-│-│-│-│-┃                1  left only   (33.33%)
   ┠─┼─┼─┼─┼─┨╌╌╌┏━┯━┯━┯━┯━┓╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╮
   ┃ │ │ │ │ ┃ = ┃ │ │ │ │ ┃  1  equal       (50.00%)  │
   ┠─┼─┼─┼─┼─┨╌╌╌┠─┼─┼─┼─┼─┨╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌├╴  2  joined
   ┃ │ │ │ │ ┃ ≠ ┃ │ │ │ │ ┃  1  unequal     (50.00%)  │
   ┗━┷━┷━┷━┷━┛╌╌╌┠─┼─┼─┼─┼─┨╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╯
                 ┃+│+│+│+│+┃  1  right only  (33.33%)
                 ┗━┷━┷━┷━┷━┛

 Columns
 ▔▔▔▔▔▔▔
   ┌───────┬────────┬───────────────────────────┐
   │ value │ 50.00% │ 2.0 -> 2.5 (1x, e.g. "b") │
   └───────┴────────┴───────────────────────────┘
```

## Next Steps

- Follow the [Quickstart Guide](guides/quickstart.ipynb) for a hands-on introduction
- Learn about [Features](guides/features/index.md) like summaries, tolerances, and investigation tools
- Check the [API Reference](api/index.rst) for detailed function documentation
