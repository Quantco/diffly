# Command Line Interface

`diffly` includes a built-in CLI for comparing parquet files directly from the terminal.

```{note}
The CLI requires [typer](https://typer.tiangolo.com/) to be installed. You can install it with `pip install typer` or `pixi add typer`.
```

We continue with the supermarket data pipeline scenario from the previous guides. The two data loads have been saved as parquet files:

- `previous_load.parquet` — the previous data load
- `current_load.parquet` — the current data load

## Basic usage

```bash
diffly previous_load.parquet current_load.parquet
```

This compares two parquet files and prints a formatted summary:

```text
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃                                     Diffly Summary                                     ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
   Attention: the data frames do not match exactly, but as no primary key columns are
   provided, the row and column matches cannot be computed.

 Schemas
 ▔▔▔▔▔▔▔
   Schemas match exactly (column count: 10).

 Rows
 ▔▔▔▔
   The number of rows matches exactly (row count: 12).
```

Without a primary key, `diffly` can only compare schemas and row counts. To enable row-level comparison, specify a primary key.

## Specifying a primary key

To enable row-level comparison, specify one or more primary key columns:

```bash
diffly previous_load.parquet current_load.parquet --primary-key transaction_id
```

```text
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃                                     Diffly Summary                                     ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
   Primary key: transaction_id

 Schemas
 ▔▔▔▔▔▔▔
   Schemas match exactly (column count: 10).

 Rows
 ▔▔▔▔
   Left count             Right count
       12     (no change)     12

   ┏━┯━┯━┯━┯━┓
   ┃-│-│-│-│-┃                2  left only   (16.67%)
   ┠─┼─┼─┼─┼─┨╌╌╌┏━┯━┯━┯━┯━┓╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╮
   ┃ │ │ │ │ ┃ = ┃ │ │ │ │ ┃  6  equal       (60.00%)  │
   ┠─┼─┼─┼─┼─┨╌╌╌┠─┼─┼─┼─┼─┨╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌├╴  10  joined
   ┃ │ │ │ │ ┃ ≠ ┃ │ │ │ │ ┃  4  unequal     (40.00%)  │
   ┗━┷━┷━┷━┷━┛╌╌╌┠─┼─┼─┼─┼─┨╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╯
                 ┃+│+│+│+│+┃  2  right only  (16.67%)
                 ┗━┷━┷━┷━┷━┛

 Columns
 ▔▔▔▔▔▔▔
   ┌─────────────────┬─────────┐
   │ discount        │  70.00% │
   │ loyalty_card_id │  90.00% │
   │ product         │ 100.00% │
   │ quantity        │ 100.00% │
   │ register_id     │ 100.00% │
   │ store_id        │ 100.00% │
   │ timestamp       │ 100.00% │
   │ total           │  70.00% │
   │ unit_price      │  70.00% │
   └─────────────────┴─────────┘
```

## Options

The CLI exposes the same options as `compare_frames()` and `summary()`. Run `diffly --help` to see all available options:

```bash
diffly --help
```
