# Tolerances

When comparing DataFrames, floating point and temporal values may differ slightly due to precision or rounding. `diffly` supports configurable tolerances to handle these cases.

## Numerical tolerances

Floating point comparison uses the same logic as Python's {func}`math.isclose`, with configurable absolute and relative tolerances.

```python
from diffly import compare_frames

comparison = compare_frames(
    left,
    right,
    primary_key="id",
    abs_tol=1e-06,
    rel_tol=1e-04,
)
```

## Temporal tolerances

For date and datetime columns, `abs_tol_temporal` specifies the maximum allowed difference:

```python
import datetime as dt

comparison = compare_frames(
    left,
    right,
    primary_key="id",
    abs_tol_temporal=dt.timedelta(seconds=1),
)
```

## Per-column tolerances

Tolerances can be specified per column by passing a mapping:

```python
comparison = compare_frames(
    left,
    right,
    primary_key="id",
    abs_tol={"price": 0.01, "quantity": 0},
    rel_tol={"price": 0, "quantity": 0.001},
)
```

When using a mapping, a value must be provided for every non-primary-key column present in both DataFrames. REVIEW: We should mention an ergonomic way of doing this in case there are many columns.

```{note}
Tolerances are not supported for `List` and `Array` columns. These are always compared for exact equality.
```
