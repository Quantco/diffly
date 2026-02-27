# Testing Utilities

`diffly` provides assertion functions that print detailed summaries when data frames differ, making test failures easier to debug.

## Asserting equality of frames

Use {func}`~diffly.testing.assert_frame_equal` to compare two Polars data frames or lazy frames in your tests:

```python
from diffly.testing import assert_frame_equal

def test_transformation(snapshot):
    result = my_transformation(input_df)
    assert_frame_equal(result, snapshot, primary_key="id")
```

When the assertion fails, `diffly` prints a summary showing exactly what differs between the frames.

### Comparison with `polars`

Unlike `polars.testing.assert_frame_equal`, `diffly`'s version:

- Prints a comprehensive summary of all differences
- Supports tolerance-based comparisons for floating point and temporal values
- Allows mixing eager and lazy frames in the same comparison

## Asserting equality of `dataframely` collections

```{note}
This function requires [dataframely](https://github.com/quantco/dataframely) to be installed.
```

Use {func}`~diffly.testing.assert_collection_equal` to compare two `dataframely` collections:

```python
from diffly.testing import assert_collection_equal

def test_pipeline(snapshot):
    result = my_pipeline(input_collection)
    assert_collection_equal(result, snapshot)
```

Two collections are considered equal if they are instances of the same collection type, have the same members, and all members are equal. The primary key for each member is automatically inferred from the collection schema.
