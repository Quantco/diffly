# Add `Digest` dataclass for machine-readable comparison output

## Context

Currently, `DataFrameComparison.summary()` returns a `Summary` object that renders rich-formatted console output. There's no way to get structured, machine-readable data from a comparison (e.g., for LLM consumption, CI pipelines, or programmatic analysis). This adds a `digest()` method returning a plain dataclass hierarchy that can be serialized via `dataclasses.asdict()` and `json.dumps()`.

## Dataclass Structure

All dataclasses in a new file `diffly/digest.py`:

```python
@dataclass
class Digest:
    equal: bool
    left_name: str
    right_name: str
    primary_key: list[str] | None
    schemas: DigestSchemas | None       # None when equal, or slim + schemas match
    rows: DigestRows | None             # None when equal, or slim + rows match
    columns: list[DigestColumn] | None  # None when equal, no PK, no joined rows, or slim + all match
    sample_rows_left_only: list[tuple[Any, ...]] | None   # None when no PK or sample_k==0
    sample_rows_right_only: list[tuple[Any, ...]] | None

@dataclass
class DigestSchemas:
    left_only: list[tuple[str, str]]                # (col_name, dtype_str)
    in_common: list[tuple[str, str, str]]            # (col_name, left_dtype_str, right_dtype_str)
    right_only: list[tuple[str, str]]

@dataclass
class DigestRows:
    n_left: int
    n_right: int
    n_left_only: int | None       # None when no primary key
    n_joined_equal: int | None
    n_joined_unequal: int | None
    n_right_only: int | None

@dataclass
class DigestColumn:
    name: str
    match_rate: float
    changes: list[DigestColumnChange] | None  # None when top_k==0 or column is hidden

@dataclass
class DigestColumnChange:
    old: Any
    new: Any
    count: int
    sample_pk: Any | None   # None when show_sample_primary_key_per_change=False
```

**Design notes:**

- `primary_key` is a top-level field so consumers know what the sample row tuples represent.
- `sample_rows_left_only` / `sample_rows_right_only` use `list[tuple]` matching the primary key column order.
- `in_common` uses 3-tuples `(name, left_dtype, right_dtype)` to capture dtype changes (when they match, `left_dtype == right_dtype`).
- `schemas` is always populated (not `None`) when frames aren't equal and not slim-hidden, even if schemas match -- the caller might want to confirm schemas are identical. **Actually**: mirror `Summary` logic -- `None` when `slim=True` and schemas are equal.

## Files to modify

### 1. New: [diffly/digest.py](diffly/digest.py)

- All dataclass definitions above
- `_to_python(value)` helper to convert Polars values (date, datetime, timedelta, Decimal) to JSON-safe types
- Builder function `_build_digest(comparison, **params) -> Digest` containing the logic to extract data from `DataFrameComparison`, mirroring the control flow of `Summary._print_to_console` / `_print_diff`
- `to_dict()` method on `Digest` via `dataclasses.asdict()`
- `to_json()` convenience method

### 2. [diffly/comparison.py](diffly/comparison.py) (~line 976)

- Add `digest()` method on `DataFrameComparison` with same signature as `summary()`
- Lazy import `from .digest import Digest` (same pattern as summary)

### 3. [diffly/cli.py](diffly/cli.py)

- Add `--json` flag (bool, default False)
- When True, call `comparison.digest(...).to_json()` instead of `comparison.summary(...).format()`

### 4. [diffly/**init**.py](diffly/__init__.py)

- No changes needed -- `Digest` is accessed via `comparison.digest()`, not imported directly. Can revisit later.

### 5. **No changes to** [diffly/testing.py](diffly/testing.py)

- `testing.py` uses `summary()` for human-readable assertion error messages. `digest()` is a data output format, not relevant to assertions.

### 6. New: [tests/test_digest.py](tests/test_digest.py)

- Equal frames -> `equal=True`, all sections `None`
- Schema differences (left-only, right-only, dtype mismatches in in_common)
- Row counts with and without primary key
- Column match rates with `show_perfect_column_matches=True/False`
- `top_k_column_changes` + `show_sample_primary_key_per_change`
- `sample_k_rows_only` for `sample_rows_left_only` / `sample_rows_right_only`
- `slim=True` suppresses matching sections
- `hidden_columns` hides column changes
- Validation errors (same as Summary: hidden PK columns, sample PK without top-k)
- JSON serialization roundtrip: `json.loads(digest.to_json())` is valid

## Verification

```bash
pixi run pytest tests/test_digest.py -v
pixi run test
pixi run pre-commit-run
```
