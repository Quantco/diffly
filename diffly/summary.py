# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import dataclasses
import io
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, cast

import polars as pl
from rich import box
from rich.columns import Columns as RichColumns
from rich.console import Console, Group, RenderableType
from rich.padding import Padding
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ._utils import Side, capitalize_first
from .metrics import Metric

if TYPE_CHECKING:  # pragma: no cover
    from .comparison import DataFrameComparison

WIDTH = 90
SCHEMAS_COLUMN_WIDTH = 25
COLUMN_SECTION_COLUMN_WIDTH = WIDTH - 15
CUSTOM_COLUMN_NAME_MAX_LENGTH = 15
OVERFLOW: Literal["crop", "fold", "ellipsis"] = "fold"
MAX_DISPLAYED_COLUMNS_IN_SAMPLE_TABLES = 5
MAX_STRING_LENGTH: int | None = 128


# ---------------------------------------------------------------------------- #
#                                    SUMMARY                                   #
# ---------------------------------------------------------------------------- #


class Summary:
    """Container object for generating a summary of the comparison of two data frames.

    Note:
        Do not initialize this object directly. Instead, use
        :meth:`DataFrameComparison.summary`.
    """

    def __init__(
        self,
        comparison: DataFrameComparison,
        show_perfect_column_matches: bool,
        top_k_column_changes: int,
        sample_k_rows_only: int,
        show_sample_primary_key_per_change: bool,
        left_name: str,
        right_name: str,
        slim: bool,
        hidden_columns: list[str] | None,
        metrics: Mapping[str, Metric] | None,
    ):
        self.slim = slim
        self._data = _compute_summary_data(
            comparison,
            show_perfect_column_matches=show_perfect_column_matches,
            top_k_column_changes=top_k_column_changes,
            sample_k_rows_only=sample_k_rows_only,
            show_sample_primary_key_per_change=show_sample_primary_key_per_change,
            left_name=left_name,
            right_name=right_name,
            slim=slim,
            hidden_columns=hidden_columns,
            metrics=metrics,
        )

    def format(self, pretty: bool | None = None) -> str:
        """Format this summary for printing.

        Args:
            pretty: Whether to color the summary for the currently terminal window.
                If set to `None`, will infer from the context.
        """
        # REVIEW: I had to set force_jupyter=False here to avoid multiple output cells
        if pretty or pretty is None:
            console = Console(force_terminal=pretty, force_jupyter=False, width=WIDTH)
            with console.capture() as capture:
                self._print_to_console(console)
            summary = capture.get()

        else:
            console = Console(file=io.StringIO(), force_jupyter=False, width=WIDTH)
            self._print_to_console(console)
            summary = cast(io.StringIO, console.file).getvalue()

        return _trim_whitespaces(summary)

    def to_json(self, **kwargs: Any) -> str:
        """Serialize this summary as a JSON string.

        Args:
            **kwargs: Additional keyword arguments passed to :func:`json.dumps`
                (e.g. ``indent=2`` for pretty-printing).

        Example:
            .. code-block:: json

                {
                  "equal": false,
                  "left_name": "left",
                  "right_name": "right",
                  "primary_key": ["id"],
                  "schemas": {
                    "left_only_names": [],
                    "in_common": [["id", "Int64", "Int64"], ["value", "Float64", "Float64"]],
                    "right_only_names": []
                  },
                  "rows": {
                    "n_left": 3,
                    "n_right": 3,
                    "n_left_only": 0,
                    "n_joined_equal": 2,
                    "n_joined_unequal": 1,
                    "n_right_only": 0
                  },
                  "columns": [
                    {
                      "name": "value",
                      "match_rate": 0.667,
                      "n_total_changes": 1,
                      "changes": [{"old": 1.0, "new": 2.0, "count": 1, "sample_pk": [1]}]
                    }
                  ],
                  "sample_rows_left_only": [],
                  "sample_rows_right_only": []
                }
        """
        return self._data.to_json(**kwargs)

    # -------------------------------- DUNDER METHODS -------------------------------- #

    def __str__(self) -> str:
        return self.format(pretty=False)

    def __repr__(self) -> str:
        return self.format(pretty=True)

    # -------------------------------------------------------------------------------- #
    #                                     RENDERING                                    #
    # -------------------------------------------------------------------------------- #

    def _print_to_console(self, console: Console) -> None:
        if not self.slim:
            console.print(
                Panel(
                    Text("Diffly Summary", style="bold", justify="center"),
                    box=box.HEAVY,
                )
            )
        if self._data.equal:
            self._print_equal(console)
        else:
            self._print_diff(console)

    def _print_equal(self, console: Console) -> None:
        if self._data._is_empty:
            message = "--- Data frames are empty, but their schema matches exactly! ---"
        else:
            message = "--- Data frames match exactly! ---"
        text = Text(message, style="green bold", justify="center")
        text.align("center", console.width)
        console.print(text)

    def _print_diff(self, console: Console) -> None:
        self._print_primary_key(console)
        self._print_schemas(console)
        self._print_rows(console)
        self._print_columns(console)
        self._print_sample_rows_only_one_side(console, side=Side.LEFT)
        self._print_sample_rows_only_one_side(console, side=Side.RIGHT)

    # --------------------------------- PRIMARY KEY ---------------------------------- #

    def _print_primary_key(self, console: Console) -> None:
        if (primary_key := self._data.primary_key) is not None:
            content = self._section_primary_key()
        else:
            content = Text(
                "Attention: the data frames do not match exactly, but as no primary"
                " key columns are provided, the row and column matches cannot be"
                " computed.",
                style="italic",
            )
        # NOTE: The primary key is only displayed in the default mode. If a primary key
        # was not supplied, the warning is displayed in both modes.
        if not self.slim or primary_key is None:
            console.print(Padding(content, pad=(0, 3)))
            console.print("")

    def _section_primary_key(self) -> RenderableType:
        primary_key = self._data.primary_key
        assert primary_key is not None
        return Group(
            f"Primary key: {', '.join(_format_colname(col) for col in primary_key)}"
        )

    # ------------------------------------ SCHEMA ------------------------------------ #

    def _print_schemas(self, console: Console) -> None:
        schemas = self._data.schemas
        if schemas is None:
            return

        content: RenderableType
        if schemas._equal:
            num_cols = len(schemas.in_common)
            content = Text(
                f"Schemas match exactly (column count: {num_cols:,}).", style="italic"
            )
        else:
            content = self._section_schemas()

        _print_section(console, "Schemas", content)

    def _section_schemas(self) -> RenderableType:
        schemas = self._data.schemas
        assert schemas is not None

        def _print_num_columns(n: int) -> str:
            return f"{n:,} column{'s' if n != 1 else ''}"

        table = Table()

        left_only_names = set(schemas.left_only_names)
        right_only_names = set(schemas.right_only_names)
        max_column_width = max(
            len(column) for column in left_only_names | right_only_names | {""}
        )

        if len(missing := left_only_names | right_only_names) > 0:
            # NOTE: At least 10 as "in common" already has 9 chars
            min_width = max(10, *[len(col) for col in missing])
        else:
            min_width = 0

        table_data: dict[str, list[str]] = {}

        # Left only
        if len(left_only_names) > 0:
            left_only_header = f"{capitalize_first(self._data._truncated_left_name)} only \n{_print_num_columns(len(left_only_names))}"
            table.add_column(
                left_only_header,
                header_style="red",
                justify="center",
                min_width=min_width,
                max_width=SCHEMAS_COLUMN_WIDTH,
                overflow=OVERFLOW,
            )
            table_data[left_only_header] = [
                _format_colname(col) for col in sorted(left_only_names)
            ]

        # In common
        in_common_header = f"In common \n{_print_num_columns(len(schemas.in_common))}"
        table.add_column(
            in_common_header,
            justify="center",
            min_width=min_width,
            max_width=SCHEMAS_COLUMN_WIDTH,
            overflow=OVERFLOW,
        )
        num_in_common = len(schemas.in_common)
        table_data[in_common_header] = []
        common_but_mismatching = schemas._mismatching_dtypes
        if len(common_but_mismatching) == 0:
            table_data[in_common_header] = ["..."]
            max_column_width = max(
                max_column_width, len(table_data[in_common_header][0])
            )
        else:
            for col, left_dtype, right_dtype in sorted(
                common_but_mismatching, key=lambda x: x[0]
            ):
                table_data[in_common_header].append(
                    f"{_format_colname(col)} [{left_dtype} -> {right_dtype}]"
                )
                max_column_width = max(
                    max_column_width, len(f"{col} [{left_dtype} -> {right_dtype}]")
                )
            num_remaining = num_in_common - len(common_but_mismatching)
            if num_remaining > 0:
                table_data[in_common_header].append(
                    f"(+{_print_num_columns(num_remaining)} with matching "
                    f"data type{'s' if num_remaining != 1 else ''})",
                )
                max_column_width = max(
                    max_column_width, len(table_data[in_common_header][-1])
                )

        # Right only
        if len(right_only_names) > 0:
            right_only_header = f"{capitalize_first(self._data._truncated_right_name)} only\n{_print_num_columns(len(right_only_names))}"
            table.add_column(
                right_only_header,
                header_style="green",
                justify="center",
                min_width=min_width,
                max_width=SCHEMAS_COLUMN_WIDTH,
                overflow=OVERFLOW,
            )
            table_data[right_only_header] = [
                _format_colname(col) for col in sorted(right_only_names)
            ]

        max_len = max(len(column_list) for column_list in table_data.values())
        table_data = {k: v + [""] * (max_len - len(v)) for k, v in table_data.items()}

        for i in range(max_len):
            table.add_row(*(v[i] for v in table_data.values()))

        if max_column_width > SCHEMAS_COLUMN_WIDTH:
            table.show_lines = True

        return table

    # ------------------------------------- ROWS ------------------------------------- #

    def _print_rows(self, console: Console) -> None:
        if self._data.rows is None:
            return

        content: RenderableType
        if self._data.primary_key is None:
            content = self._render_rows_without_primary_key()
        else:
            content = self._render_rows_with_primary_key()
        _print_section(console, "Rows", content)

    def _render_rows_without_primary_key(self) -> RenderableType:
        rows = self._data.rows
        assert rows is not None
        content: RenderableType
        if rows._equal_num_rows:
            content = Text(
                f"The number of rows matches exactly (row count: {rows.n_left:,}).",
                style="italic",
            )
        else:
            content = self._section_row_counts()
        return content

    def _render_rows_with_primary_key(self) -> RenderableType:
        rows = self._data.rows
        assert rows is not None
        assert rows.n_joined_equal is not None
        assert rows.n_joined_unequal is not None
        assert rows.n_left_only is not None
        assert rows.n_right_only is not None

        content: RenderableType
        if rows._equal_rows:
            content = Text(
                f"All rows match exactly (row count: {rows.n_left:,}).",
                style="italic",
            )
        else:
            if not rows._show_row_counts:
                content = Group(self._section_row_matches())
            else:
                content = Group(
                    self._section_row_counts(),
                    "",
                    self._section_row_matches(),
                )
        return content

    def _section_row_counts(self) -> RenderableType:
        rows = self._data.rows
        assert rows is not None
        gain_loss = ""
        if rows.n_left > 0:
            fraction_rows_right = rows.n_right / rows.n_left
            if fraction_rows_right > 1:
                gain_loss = f"(+{(fraction_rows_right - 1):.2%})"
            elif fraction_rows_right < 1:
                gain_loss = f"(-{(1 - fraction_rows_right):.2%})"
            else:
                gain_loss = "(no change)"

        ### Row counts
        count_rows: list[RenderableType] = []

        count_grid = Table(padding=0, box=None)
        left_header = f"{capitalize_first(self._data._truncated_left_name)} count"
        right_header = f"{capitalize_first(self._data._truncated_right_name)} count"
        count_grid.add_column(left_header, justify="center")
        count_grid.add_column("", justify="center")
        count_grid.add_column(right_header, justify="center")
        count_grid.add_row(
            f"{rows.n_left:,}",
            f" {gain_loss} ",
            f"{rows.n_right:,}",
        )
        count_rows.append(count_grid)

        return Group(*count_rows)

    def _section_row_matches(self) -> RenderableType:
        rows = self._data.rows
        assert rows is not None
        assert rows.n_left_only is not None
        assert rows.n_joined_equal is not None
        assert rows.n_joined_unequal is not None
        assert rows.n_right_only is not None
        n_joined = rows.n_joined_equal + rows.n_joined_unequal

        columns: list[RenderableType] = []
        num_dummy_cols = 5

        # Left Table
        if rows.n_left > 0:
            left_table = Table(show_header=False, padding=0, box=box.HEAVY_EDGE)
            for _ in range(num_dummy_cols):
                left_table.add_column()
            if rows.n_left_only > 0:
                left_table.add_row(*([Text("-", style="red")] * num_dummy_cols))
                left_table.add_section()
            if rows.n_joined_equal > 0:
                left_table.add_row(*([" "] * num_dummy_cols))
                left_table.add_section()
            if rows.n_joined_unequal > 0:
                left_table.add_row(*([" "] * num_dummy_cols))
                left_table.add_section()

            columns.append(left_table)

        # Separator between tables
        if n_joined > 0:
            separator_rows: list[RenderableType] = []
            if rows.n_left_only > 0:
                separator_rows.append("\n")
            if rows.n_joined_equal > 0:
                separator_rows.append("╌" * 3)
                separator_rows.append(Text(" = ", style="bold"))
            if rows.n_joined_unequal > 0:
                separator_rows.append("╌" * 3)
                separator_rows.append(Text(" ≠ ", style="bold"))
            separator_rows.append("╌" * 3)

            columns.append(Group(*separator_rows))
        else:
            columns.append(" " * 3)

        # Right table
        if rows.n_right > 0:
            right_table = Table(show_header=False, padding=0, box=box.HEAVY_EDGE)
            for _ in range(num_dummy_cols):
                right_table.add_column()
            if rows.n_joined_equal > 0:
                right_table.add_row(*([" "] * num_dummy_cols))
                right_table.add_section()
            if rows.n_joined_unequal > 0:
                right_table.add_row(*([" "] * num_dummy_cols))
                right_table.add_section()
            if rows.n_right_only > 0:
                right_table.add_row(*([Text("+", style="green")] * num_dummy_cols))

            if rows.n_left_only > 0:
                columns.append(Group("\n", right_table))
            else:
                columns.append(right_table)

        # Numbers for groups
        if rows.n_left > 0 or rows.n_right > 0:
            grid = Table(
                show_header=False,
                box=box.Box(
                    "\n".join(
                        (  # header row
                            ["╌" * 4]
                            if (rows.n_left_only == 0 and rows.n_left > 0)
                            else [" " * 4]
                        )
                        + [" " * 4] * 3
                        + ["╌" * 4]
                        + [" " * 4] * 2
                        + (  # bottom row
                            ["╌" * 4]
                            if (rows.n_right_only == 0 and rows.n_right > 0)
                            else [" " * 4]
                        )
                    )
                ),
                padding=(0, 0, 0, 1),
            )
            grid.add_column("Count", justify="right")
            grid.add_column("Type", justify="left")
            grid.add_column("Percentage", justify="right")
            if rows.n_left_only > 0:
                fraction_left_only = rows.n_left_only / rows.n_left
                grid.add_row(
                    f"{rows.n_left_only:,}",
                    f"{self._data._truncated_left_name} only",
                    f"({_format_fraction_as_percentage(fraction_left_only)})",
                )
                grid.add_section()
            if rows.n_joined_equal > 0:
                fraction_equal = rows.n_joined_equal / n_joined
                grid.add_row(
                    f"{rows.n_joined_equal:,}",
                    "equal",
                    f"({_format_fraction_as_percentage(fraction_equal)})",
                )
                grid.add_section()
            if rows.n_joined_unequal > 0:
                fraction_unequal = rows.n_joined_unequal / n_joined
                grid.add_row(
                    f"{rows.n_joined_unequal:,}",
                    "unequal",
                    f"({_format_fraction_as_percentage(fraction_unequal)})",
                )
                grid.add_section()
            if rows.n_right_only > 0:
                fraction_right_only = rows.n_right_only / rows.n_right
                grid.add_row(
                    f"{rows.n_right_only:,}",
                    f"{self._data._truncated_right_name} only",
                    f"({_format_fraction_as_percentage(fraction_right_only)})",
                )
            columns.append(grid)

        # Num joined
        num_sections = (rows.n_joined_equal > 0) + (rows.n_joined_unequal > 0)
        if num_sections > 0:
            joined_rows: list[RenderableType] = []
            if rows.n_left_only > 0:
                joined_rows.append("\n")
            joined_rows.append("╌╮")
            joined_rows.append(" │")
            if num_sections > 1:
                joined_rows.append(f"╌├╴  {n_joined:,}  joined")
                joined_rows.append(" │")
            joined_rows.append("╌╯")
            columns.append(Group(*joined_rows))

        return RichColumns(columns, padding=0)

    # -------------------------------- COLUMN MATCHES -------------------------------- #

    def _print_columns(self, console: Console) -> None:
        if self._data.columns is None:
            return
        _print_section(
            console,
            "Columns",
            self._section_columns(),
        )

    def _section_columns(self) -> RenderableType:
        display_items: list[RenderableType] = []
        columns = self._data.columns
        assert columns is not None

        if not self._data._other_common_columns:
            display_items.append(
                Text("No common non-primary key columns to compare.", style="italic")
            )
        elif not columns:
            display_items.append(Text("All columns match perfectly.", style="italic"))
        else:
            metric_labels = self._data._metric_labels
            matches = Table(show_header=bool(metric_labels))
            matches.add_column(
                "Column",
                max_width=COLUMN_SECTION_COLUMN_WIDTH,
                overflow=OVERFLOW,
            )
            matches.add_column("Match Rate", justify="right")
            for label in metric_labels:
                matches.add_column(label, justify="right")
            has_top_changes_column = any(
                c.changes is not None for c in columns if c.match_rate < 1
            )
            if has_top_changes_column:
                matches.add_column("Top Changes", justify="right")
            max_col_len = max(len(c.name) for c in columns)
            for col in columns:
                row_items: list[RenderableType] = [
                    Text(col.name, style="cyan"),
                    f"{_format_fraction_as_percentage(col.match_rate)}",
                ]
                for label in metric_labels:
                    value = col.metrics.get(label) if col.metrics else None
                    row_items.append(_format_metric_value(value))
                if col.changes is not None:
                    change_lines = []
                    for change in col.changes:
                        line = (
                            f"{_format_value(change.old)} -> "
                            f"{_format_value(change.new)} ({change.count:,}x"
                        )
                        if change.sample_pk is not None:
                            line += ", e.g. "
                            if len(change.sample_pk) == 1:
                                line += _format_value(change.sample_pk[0])
                            else:
                                line += "("
                                line += ", ".join(
                                    [_format_value(v) for v in change.sample_pk]
                                )
                                line += ")"
                        line += ")"
                        change_lines.append(line)

                    remaining_count = col.n_total_changes - len(col.changes)
                    if remaining_count > 0:
                        change_lines.append(
                            f"(...and {remaining_count:,} {('other' if remaining_count == 1 else 'others')})"
                        )

                    text = "\n".join(change_lines)
                    row_items.append(text)

                matches.add_row(*row_items)
                if has_top_changes_column or max_col_len > COLUMN_SECTION_COLUMN_WIDTH:
                    matches.add_section()

            display_items.append(matches)

        return Group(*display_items)

    # ------------------------------ ROWS ONLY ONE SIDE ------------------------------ #

    def _print_sample_rows_only_one_side(self, console: Console, side: Side) -> None:
        if side == Side.LEFT:
            sample_rows = self._data.sample_rows_left_only
            name = self._data._truncated_left_name
        else:
            sample_rows = self._data.sample_rows_right_only
            name = self._data._truncated_right_name

        primary_key = self._data.primary_key
        if primary_key is not None and sample_rows is not None and len(sample_rows) > 0:
            _print_section(
                console,
                f"Rows {name} only",
                self._section_rows_only_one_side(side),
            )

    def _section_rows_only_one_side(self, side: Side) -> RenderableType:
        if side == Side.LEFT:
            sample_rows = self._data.sample_rows_left_only
        else:
            sample_rows = self._data.sample_rows_right_only
        assert sample_rows is not None
        primary_key = self._data.primary_key
        assert primary_key is not None
        table = Table()
        for col in primary_key[:MAX_DISPLAYED_COLUMNS_IN_SAMPLE_TABLES]:
            table.add_column(col, overflow="ellipsis")

        if len(primary_key) > MAX_DISPLAYED_COLUMNS_IN_SAMPLE_TABLES:
            table.add_column("...", style="dim")

        for row in sample_rows:
            added_row = [str(v) for v in row[:MAX_DISPLAYED_COLUMNS_IN_SAMPLE_TABLES]]
            if len(primary_key) > MAX_DISPLAYED_COLUMNS_IN_SAMPLE_TABLES:
                added_row.append("...")
            table.add_row(*added_row)

        return table


# ---------------------------------------------------------------------------- #
#                                 SUMMARY DATA                                 #
# ---------------------------------------------------------------------------- #


@dataclass
class SummaryDataSchemas:
    left_only_names: list[str]
    in_common: list[tuple[str, str, str]]
    right_only_names: list[str]
    _equal: bool
    _mismatching_dtypes: list[tuple[str, str, str]]


@dataclass
class SummaryDataRows:
    n_left: int
    n_right: int
    n_left_only: int | None
    n_joined_equal: int | None
    n_joined_unequal: int | None
    n_right_only: int | None
    _equal_rows: bool
    _equal_num_rows: bool
    _show_row_counts: bool


@dataclass
class SummaryDataColumnChange:
    old: Any
    new: Any
    count: int
    sample_pk: tuple[Any, ...] | None


@dataclass
class SummaryDataColumn:
    name: str
    match_rate: float
    n_total_changes: int
    changes: list[SummaryDataColumnChange] | None
    metrics: dict[str, Any] | None = None


@dataclass
class SummaryData:
    equal: bool
    left_name: str | None
    right_name: str | None
    primary_key: list[str] | None
    schemas: SummaryDataSchemas | None
    rows: SummaryDataRows | None
    columns: list[SummaryDataColumn] | None
    sample_rows_left_only: list[tuple[Any, ...]] | None
    sample_rows_right_only: list[tuple[Any, ...]] | None
    _is_empty: bool
    _other_common_columns: list[str]
    _truncated_left_name: str
    _truncated_right_name: str
    _metric_labels: list[str] = dataclasses.field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        def _convert(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {k: _convert(v) for k, v in obj.items() if not k.startswith("_")}
            if isinstance(obj, (list, tuple)):
                return type(obj)(_convert(v) for v in obj)
            return to_json_safe(obj)

        return _convert(dataclasses.asdict(self))

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)


def to_json_safe(value: Any) -> Any:
    """Convert values to JSON-safe Python types."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _compute_summary_data(
    comparison: DataFrameComparison,
    show_perfect_column_matches: bool,
    top_k_column_changes: int,
    sample_k_rows_only: int,
    show_sample_primary_key_per_change: bool,
    left_name: str,
    right_name: str,
    slim: bool,
    hidden_columns: list[str] | None,
    metrics: Mapping[str, Metric] | None,
) -> SummaryData:
    from .comparison import DataFrameComparison

    hidden_columns = hidden_columns or []

    def _validate_primary_key_hidden_columns() -> None:
        overlap = sorted(
            set(hidden_columns).intersection(set(comparison.primary_key or []))
        )
        if overlap and sample_k_rows_only > 0:
            raise ValueError(
                f"Cannot show sample rows only on the left or right side when primary"
                f" key column(s) {', '.join(overlap)} should be hidden."
            )
        if overlap and show_sample_primary_key_per_change:
            raise ValueError(
                f"Cannot show sample primary key for changed columns when primary"
                f" key column(s) {', '.join(overlap)} should be hidden."
            )

    _validate_primary_key_hidden_columns()
    if top_k_column_changes == 0 and show_sample_primary_key_per_change:
        raise ValueError(
            "Cannot show sample primary key per change when top_k_column_changes is 0."
        )

    top_k_changes_by_column = {
        col: 0 if col in hidden_columns else top_k_column_changes
        for col in comparison._other_common_columns
    }
    comp = DataFrameComparison(
        left=comparison.left.collect().lazy(),
        right=comparison.right.collect().lazy(),
        left_schema=comparison.left_schema,
        right_schema=comparison.right_schema,
        primary_key=comparison.primary_key,
        _other_common_columns=comparison._other_common_columns,
        abs_tol_by_column=comparison.abs_tol_by_column,
        rel_tol_by_column=comparison.rel_tol_by_column,
        abs_tol_temporal_by_column=comparison.abs_tol_temporal_by_column,
    )

    is_equal = comp.equal()
    is_empty = comp.num_rows_left() == 0

    truncated_left = _truncate_name(left_name)
    truncated_right = _truncate_name(right_name)

    if is_equal:
        return SummaryData(
            equal=True,
            left_name=None,
            right_name=None,
            primary_key=None,
            schemas=None,
            rows=None,
            columns=None,
            sample_rows_left_only=None,
            sample_rows_right_only=None,
            _is_empty=is_empty,
            _other_common_columns=comp._other_common_columns,
            _truncated_left_name=truncated_left,
            _truncated_right_name=truncated_right,
        )

    metrics_resolved: dict[str, Metric] = dict(metrics or {})
    metrics_by_column = _compute_column_metrics(comp, metrics_resolved)
    metric_labels = list(metrics_resolved.keys())

    schemas = _compute_schemas(comp, slim)
    rows = _compute_rows(comp, slim)
    columns = _compute_columns(
        comp,
        slim,
        show_perfect_column_matches,
        top_k_changes_by_column,
        show_sample_primary_key_per_change,
        metrics_by_column,
    )
    sample_rows_left_only, sample_rows_right_only = _compute_sample_rows(
        comp, sample_k_rows_only
    )

    return SummaryData(
        equal=False,
        left_name=left_name,
        right_name=right_name,
        primary_key=comp.primary_key,
        schemas=schemas,
        rows=rows,
        columns=columns,
        sample_rows_left_only=sample_rows_left_only,
        sample_rows_right_only=sample_rows_right_only,
        _is_empty=is_empty,
        _other_common_columns=comp._other_common_columns,
        _truncated_left_name=truncated_left,
        _truncated_right_name=truncated_right,
        _metric_labels=metric_labels,
    )


def _compute_schemas(
    comp: DataFrameComparison, slim: bool
) -> SummaryDataSchemas | None:
    # NOTE: In slim mode, we only print the section if there are differences.
    if slim and comp.schemas.equal():
        return None
    in_common = sorted(comp.schemas.in_common().items())
    mismatching = sorted(comp.schemas.in_common().mismatching_dtypes().items())
    return SummaryDataSchemas(
        left_only_names=sorted(comp.schemas.left_only().column_names()),
        in_common=[
            (name, str(left_dtype), str(right_dtype))
            for name, (left_dtype, right_dtype) in in_common
        ],
        right_only_names=sorted(comp.schemas.right_only().column_names()),
        _equal=comp.schemas.equal(),
        _mismatching_dtypes=[
            (name, str(left_dtype), str(right_dtype))
            for name, (left_dtype, right_dtype) in mismatching
        ],
    )


def _compute_rows(comp: DataFrameComparison, slim: bool) -> SummaryDataRows | None:
    if comp.primary_key is not None:
        rows_equal = comp._equal_rows()
    else:
        rows_equal = comp.equal_num_rows()
    # NOTE: In slim mode, we only print the section if there are differences.
    if slim and rows_equal:
        return None
    if comp.primary_key is not None:
        return SummaryDataRows(
            n_left=comp.num_rows_left(),
            n_right=comp.num_rows_right(),
            n_left_only=comp.num_rows_left_only(),
            n_joined_equal=comp.num_rows_joined_equal(),
            n_joined_unequal=comp.num_rows_joined_unequal(),
            n_right_only=comp.num_rows_right_only(),
            _equal_rows=comp._equal_rows(),
            _equal_num_rows=comp.equal_num_rows(),
            # NOTE: In slim mode, we omit the row counts section and only show the
            # row matches section.
            _show_row_counts=not (comp.equal_num_rows() and slim),
        )
    return SummaryDataRows(
        n_left=comp.num_rows_left(),
        n_right=comp.num_rows_right(),
        n_left_only=None,
        n_joined_equal=None,
        n_joined_unequal=None,
        n_right_only=None,
        _equal_rows=False,
        _equal_num_rows=comp.equal_num_rows(),
        _show_row_counts=True,
    )


def _compute_column_metrics(
    comp: DataFrameComparison,
    metrics: Mapping[str, Metric],
) -> dict[str, dict[str, Any]]:
    if not metrics:
        return {}
    if comp.primary_key is None or comp.num_rows_joined() == 0:
        return {}

    numeric_cols = [
        c
        for c in comp._other_common_columns
        if comp.left_schema[c].is_numeric() and comp.right_schema[c].is_numeric()
    ]
    out: dict[str, dict[str, Any]] = {c: {} for c in numeric_cols}
    if not numeric_cols:
        return out

    joined = comp.joined(lazy=True)
    agg_exprs = [
        metric(
            pl.col(f"{c}_{Side.LEFT}"),
            pl.col(f"{c}_{Side.RIGHT}"),
        ).alias(f"{label}__{c}")
        for label, metric in metrics.items()
        for c in numeric_cols
    ]
    row = joined.select(agg_exprs).collect().row(0, named=True)
    for c in numeric_cols:
        for label in metrics:
            out[c][label] = row[f"{label}__{c}"]
    return out


def _compute_columns(
    comp: DataFrameComparison,
    slim: bool,
    show_perfect_column_matches: bool,
    top_k_changes_by_column: dict[str, int],
    show_sample_primary_key_per_change: bool,
    metrics_by_column: dict[str, dict[str, Any]],
) -> list[SummaryDataColumn] | None:
    # NOTE: We can only compute column matches if there are primary key columns and at
    # least one joined row.
    if comp.primary_key is None or comp.num_rows_joined() == 0:
        return None
    match_rates = comp.fraction_same()
    # NOTE: In slim mode, we only print the columns section if there are
    # non-primary key columns and at least one column has a match rate < 1.
    if slim and not (comp._other_common_columns and min(match_rates.values()) < 1):
        return None
    columns: list[SummaryDataColumn] = []
    for col_name in sorted(match_rates):
        rate = match_rates[col_name]
        if not show_perfect_column_matches and rate >= 1:
            continue
        top_k = top_k_changes_by_column[col_name]
        changes: list[SummaryDataColumnChange] | None = None
        n_total_changes = 0
        if top_k > 0 and rate < 1:
            all_change_counts = comp.change_counts(
                col_name,
                include_sample_primary_key=show_sample_primary_key_per_change,
            )
            n_total_changes = len(all_change_counts)
            top_change_counts = all_change_counts.head(top_k)
            changes = []
            for row in top_change_counts.iter_rows(named=True):
                sample_pk: tuple[Any, ...] | None = None
                if show_sample_primary_key_per_change:
                    pk_cols = comp.primary_key
                    assert isinstance(pk_cols, list)
                    sample_pk = tuple(row[f"sample_{c}"] for c in pk_cols)
                changes.append(
                    SummaryDataColumnChange(
                        old=row[Side.LEFT],
                        new=row[Side.RIGHT],
                        count=row["count"],
                        sample_pk=sample_pk,
                    )
                )
        columns.append(
            SummaryDataColumn(
                name=col_name,
                match_rate=rate,
                n_total_changes=n_total_changes,
                changes=changes,
                metrics=metrics_by_column.get(col_name),
            )
        )
    return columns


def _compute_sample_rows(
    comp: DataFrameComparison, sample_k_rows_only: int
) -> tuple[list[tuple[Any, ...]] | None, list[tuple[Any, ...]] | None]:
    if comp.primary_key is None or sample_k_rows_only <= 0:
        return None, None
    pk = comp.primary_key
    assert isinstance(pk, list)

    if comp.num_rows_left_only() > 0:
        df = comp.left_only(lazy=True).select(pk).head(sample_k_rows_only).collect()
        sample_left = [tuple(row) for row in df.iter_rows()]
    else:
        sample_left = []

    if comp.num_rows_right_only() > 0:
        df = comp.right_only(lazy=True).select(pk).head(sample_k_rows_only).collect()
        sample_right = [tuple(row) for row in df.iter_rows()]
    else:
        sample_right = []

    return sample_left, sample_right


# ------------------------------------------------------------------------------------ #
#                                         UTILS                                        #
# ------------------------------------------------------------------------------------ #


def _print_section(console: Console, heading: str, content: RenderableType) -> None:
    underline = "▔" * len(heading)
    console.print(
        Padding(
            Group(
                Text(heading, style="bold"),
                underline,
                Padding(content, pad=(0, 1, 0, 2)),
            ),
            pad=(0, 1, 1, 1),
        ),
    )


def _truncate_name(name: str) -> str:
    if len(name) > CUSTOM_COLUMN_NAME_MAX_LENGTH:
        return f"{name[:CUSTOM_COLUMN_NAME_MAX_LENGTH]}..."
    return name


def _format_colname(name: str) -> str:
    return f"[cyan]{name}[/cyan]"


def _format_fraction_as_percentage(fraction: float) -> str:
    """Format a fraction as a percentage with two decimal places.

    Only shows 0%/100% if the fraction is exactly 0 or 1, respectively.

    Args:
        fraction: The fraction to format as a percentage. Must be in the range [0, 1].

    Returns:
        The formatted percentage, including a percent sign.
    """
    assert 0 <= fraction <= 1, "Fraction must be between 0 and 1"
    percentage = fraction * 100
    if percentage not in [0, 100]:
        percentage = min(max(percentage, 0.01), 99.99)
    return f"{percentage:.2f}%"


def _yellow(raw: Any) -> str:
    return f"[yellow]{raw}[/yellow]"


def _format_value(value: Any) -> str:
    if isinstance(value, list):
        formatted = [_format_value(x) for x in value]
        if len(formatted) > 5:
            return f"[{', '.join(formatted[:2])}, ..., {', '.join(formatted[-2:])}]"
        return f"[{', '.join(formatted)}]"
    elif isinstance(value, str):
        if MAX_STRING_LENGTH and len(value) > MAX_STRING_LENGTH:
            offset = MAX_STRING_LENGTH // 2
            raw = f'"{value[:offset]} ... {value[-offset:]}"'
        else:
            raw = f'"{value}"'
    elif isinstance(value, date | datetime):
        raw = str(value)
    else:
        raw = value
    return _yellow(raw)


def _format_metric_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return _yellow(f"{value:.4g}")
    return _format_value(value)


def _trim_whitespaces(s: str) -> str:
    return "\n".join(line.rstrip() for line in s.splitlines())
