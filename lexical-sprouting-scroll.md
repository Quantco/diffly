# Add `SummaryData` dataclass as the data layer for comparison output

## Context

`Summary` currently both extracts data from `DataFrameComparison` and renders it with Rich — every `_print_*` method queries the comparison object directly. There is no structured, machine-readable output format. We introduce `SummaryData` as an intermediate data layer: a plain dataclass hierarchy computed once in `Summary.__init__`, then consumed for both Rich rendering (`print(summary)` / `summary.format()`) and JSON serialization (`summary.to_json()`).

## Architecture

```
DataFrameComparison.summary()
        │
        ▼
      Summary.__init__
        │
        ├── calls _compute_summary_data() once
        │           │
        │           ▼
        │      SummaryData          ← plain dataclass, no dependencies beyond stdlib
        │
        ├── print(summary) / summary.format()   → Rich rendering from SummaryData
        └── summary.to_json()                   → JSON serialization from SummaryData
```

- **`SummaryData`** is the single source of truth for what data to present given the parameters (`slim`, `show_perfect_column_matches`, `top_k_column_changes`, etc.).
- **`Summary`** computes a `SummaryData` in its `__init__` via `_compute_summary_data()`, stores it as `self._data`. All `_print_*` methods render from `self._data` instead of querying `self._comparison`. `to_json()` serializes `self._data`.
- **`comparison.summary()`** remains the only entry point. No new method on `DataFrameComparison`.

## Dataclass Design

All dataclasses live in `diffly/summary.py` alongside the existing `Summary` class:

```python
@dataclass
class SummaryData:
    equal: bool
    left_name: str
    right_name: str
    primary_key: list[str] | None
    schemas: SummaryDataSchemas | None
    rows: SummaryDataRows | None
    columns: list[SummaryDataColumn] | None
    sample_rows_left_only: list[tuple[Any, ...]] | None   # None when no PK or sample_k==0
    sample_rows_right_only: list[tuple[Any, ...]] | None # None when no PK or sample_k==0

    def to_dict(self) -> dict[str, Any]: ...
    def to_json(self, **kwargs) -> str: ...

@dataclass
class SummaryDataSchemas:
    left_only: list[tuple[str, str]]                # (col_name, dtype_str)
    in_common: list[tuple[str, str, str]]            # (col_name, left_dtype_str, right_dtype_str)
    right_only: list[tuple[str, str]]

@dataclass
class SummaryDataRows:
    n_left: int
    n_right: int
    n_left_only: int | None       # None when no primary key
    n_joined_equal: int | None  # None when no primary key
    n_joined_unequal: int | None  # None when no primary key
    n_right_only: int | None  # None when no primary key

@dataclass
class SummaryDataColumn:
    name: str
    match_rate: float
    n_total_changes: int          # total distinct changes (needed for "...and N others")
    changes: list[SummaryDataColumnChange] | None  # None when top_k==0 or column is hidden

@dataclass
class SummaryDataColumnChange:
    old: Any
    new: Any
    count: int
    sample_pk: tuple[Any, ...] | None   # None when show_sample_primary_key_per_change=False
```

### Design decisions

- **Primary key consistency:** Both `sample_rows_{left,right}_only` entries and `sample_pk` in `SummaryDataColumnChange` use `tuple[Any, ...]` matching the `primary_key` column order.
- **`n_total_changes`** on `SummaryDataColumn`: needed to render `"(...and 5 others)"`. The `changes` list only holds the top-k.
- **Equal + empty frames:** Summary distinguishes "empty but matching" from "match exactly" via row count. _Alternative:_ add a top-level `n_rows_left` field if this proves awkward during implementation.

## Files to modify

### 1. `diffly/summary.py`

**Add** (above the `Summary` class):

- `SummaryData` and child dataclass definitions
- `_to_python(value)` helper for JSON-safe conversion (date → isoformat, timedelta → total_seconds, Decimal → float)
- `_compute_summary_data(comparison, **params) -> SummaryData`: single place for data extraction, parameter validation, and "what to show" decisions. This moves the current validation logic out of `Summary.__init__` and the data-querying logic out of the `_print_*` methods.

**Modify** `Summary`:

- `__init__` calls `_compute_summary_data()`, stores result as `self._data`. Remove `self._comparison` and parameter fields that are now captured in `SummaryData`.
- Keep `self.slim` (controls header panel rendering, not data content).
- Add `to_json(**kwargs) -> str` method delegating to `self._data.to_json()`.
- Refactor each `_print_*` method to render from `self._data`:
  - `_print_to_console`: check `self._data.equal`
  - `_print_equal`: derive "empty but matching" from `self._data`
  - `_print_primary_key`: read `self._data.primary_key`
  - `_print_schemas`: render from `self._data.schemas` (skip if `None`)
  - `_print_rows`: render from `self._data.rows` (skip if `None`)
  - `_print_columns`: render from `self._data.columns` (skip if `None`)
  - `_print_sample_rows_only_one_side`: render from `self._data.sample_rows_{left,right}_only`
- Remove runtime imports of `DataFrameComparison` and `Schemas` (no longer needed for rendering)

### 2. `diffly/comparison.py`

- No changes. `summary()` continues to return `Summary` with the same signature.

### 3. `diffly/cli.py`

- Add `--json` flag (bool, default False).
- When True, call `comparison.summary(...).to_json()` instead of `comparison.summary(...).format()`.

### 4. New: `tests/test_summary_data.py`

- Parametrized test over `show_perfect_column_matches`, `top_k_column_changes`, `slim`, `sample_k_rows_only` (with derived `sample_pk`) using `itertools.product`.
- Single rich test case where all `SummaryData` fields are populated; assert correct fields are `None` vs populated per parameter combination.
- Additional tests: equal frames, no primary key, hidden columns, multiple PK, slim suppression, validation errors.
- JSON roundtrip via `json.loads(summary.to_json())`.

### 5. No changes to `diffly/__init__.py` or `diffly/testing.py`

## Verification

```bash
pixi run pytest tests/test_summary_data.py -v
pixi run test
pixi run pre-commit-run
```

Existing summary fixture tests must continue to pass unchanged — they validate that the Rich rendering is identical before and after the refactor.
