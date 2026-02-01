# Command Line Interface

`diffly` includes a built-in CLI for comparing parquet files directly from the terminal.

```{note}
The CLI requires [typer](https://typer.tiangolo.com/) to be installed. You can install it with `pip install typer` or `pixi add typer`.
```

## Basic usage

```bash
diffly left.parquet right.parquet
```

This compares two parquet files and prints a formatted summary of the differences.

## Specifying a primary key

To enable row-level comparison, specify one or more primary key columns:

```bash
diffly left.parquet right.parquet --primary-key id
```

For composite keys, use multiple `--primary-key` options:

```bash
diffly left.parquet right.parquet --primary-key id --primary-key timestamp
```

## Options

The CLI exposes the same options as `compare_frames()` and `summary()`. Run `diffly --help` to see all available options:

```bash
diffly --help
```
