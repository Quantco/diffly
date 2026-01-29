# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import io
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal, cast

import polars as pl
from rich import box
from rich.columns import Columns as RichColumns
from rich.console import Console, Group, RenderableType
from rich.padding import Padding
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ._utils import Side, capitalize_first
from .comparison import (
    DataFrameComparison,
    Schemas,
)

WIDTH = 90
SCHEMAS_COLUMN_WIDTH = 25
COLUMN_SECTION_COLUMN_WIDTH = WIDTH - 15
CUSTOM_COLUMN_NAME_MAX_LENGTH = 15
OVERFLOW: Literal["crop", "fold", "ellipsis"] = "fold"
MAX_DISPLAYED_COLUMNS_IN_SAMPLE_TABLES = 5
MAX_STRING_LENGTH: int | None = 128


@dataclass
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
    ):
        def _truncate_name(name: str) -> str:
            if len(name) > CUSTOM_COLUMN_NAME_MAX_LENGTH:
                return f"{name[:CUSTOM_COLUMN_NAME_MAX_LENGTH]}..."
            return name

        def _validate_primary_key_hidden_columns() -> None:
            overlap = set(self.hidden_columns).intersection(
                set(self._comparison.primary_key or [])
            )
            if overlap and self.sample_k_rows_only > 0:
                raise ValueError(
                    f"Cannot show sample rows only on the left or right side when primary"
                    f" key column(s) {', '.join(overlap)} should be hidden."
                )
            if overlap and self.show_sample_primary_key_per_change:
                raise ValueError(
                    f"Cannot show sample primary key for changed columns when primary"
                    f" key column(s) {', '.join(overlap)} should be hidden."
                )

        self._comparison = DataFrameComparison(
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
        self.show_perfect_column_matches = show_perfect_column_matches
        self.left_name = _truncate_name(left_name)
        self.right_name = _truncate_name(right_name)
        self.slim = slim
        self.sample_k_rows_only = sample_k_rows_only
        self.show_sample_primary_key_per_change = show_sample_primary_key_per_change
        self.hidden_columns = hidden_columns or []
        self.top_k_changes_by_column = {
            col: 0 if col in self.hidden_columns else top_k_column_changes
            for col in comparison._other_common_columns
        }
        _validate_primary_key_hidden_columns()
        if (top_k_column_changes == 0) and show_sample_primary_key_per_change:
            raise ValueError(
                "Cannot show sample primary key per change when top_k_column_changes is 0."
            )

    def format(self, pretty: bool | None = None) -> str:
        """Format this summary for printing.

        Args:
            pretty: Whether to color the summary for the currently terminal window.
                If set to `None`, will infer from the context.
        """
        if pretty or pretty is None:
            console = Console(force_terminal=pretty, width=WIDTH)
            with console.capture() as capture:
                self._print_to_console(console)
            summary = capture.get()

        else:
            console = Console(file=io.StringIO(), width=WIDTH)
            self._print_to_console(console)
            summary = cast(io.StringIO, console.file).getvalue()

        return _trim_whitespaces(summary)

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
        if self._comparison.equal():
            self._print_equal(console)
        else:
            self._print_diff(console)

    def _print_equal(self, console: Console) -> None:
        text = Text(
            "--- Data frames match exactly! ---", style="green bold", justify="center"
        )
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
        if (primary_key := self._comparison.primary_key) is not None:
            content = self._section_primary_key(primary_key)
        else:
            content = Text(
                "Attention: the data frames do not match exactly, but as no primary"
                " key columns are provided, the row and column matches cannot be"
                " computed.",
                style="italic",
            )
        # NOTE: The primary key is only displayed in the default mode. If a primary
        # key was not supplied, the warning is displayed in both modes.
        if not self.slim or primary_key is None:
            console.print(Padding(content, pad=(0, 3)))
            console.print("")

    def _section_primary_key(self, primary_key: list[str]) -> RenderableType:
        return Group(
            f"Primary key: {', '.join(_format_colname(col) for col in primary_key)}"
        )

    # ------------------------------------ SCHEMA ------------------------------------ #

    def _print_schemas(self, console: Console) -> None:
        content: RenderableType
        if self._comparison.schemas.equal():
            num_cols = len(self._comparison.schemas.left())
            content = Text(
                f"Schemas match exactly (column count: {num_cols:,}).", style="italic"
            )
        else:
            content = self._section_schemas(self._comparison.schemas)

        # NOTE: In slim mode, we only print the section if there are differences.
        if not self.slim or not self._comparison.schemas.equal():
            _print_section(console, "Schemas", content)

    def _section_schemas(self, columns: Schemas) -> RenderableType:
        def _print_num_columns(n: int) -> str:
            return f"{n:,} column{'s' if n != 1 else ''}"

        table = Table()

        left_only = columns.left_only().column_names()
        right_only = columns.right_only().column_names()
        max_column_width = max(len(column) for column in left_only | right_only | {""})

        if len(missing := left_only | right_only) > 0:
            # NOTE: At least 10 as "in common" already has 9 chars
            min_width = max(10, *[len(col) for col in missing])
        else:
            min_width = 0

        table_data: dict[str, list[str]] = {}

        # Left only
        if len(left_only) > 0:
            left_only_header = f"{capitalize_first(self.left_name)} only \n{_print_num_columns(len(left_only))}"
            table.add_column(
                left_only_header,
                header_style="red",
                justify="center",
                min_width=min_width,
                max_width=SCHEMAS_COLUMN_WIDTH,
                overflow=OVERFLOW,
            )
            table_data[left_only_header] = [
                _format_colname(col) for col in sorted(left_only)
            ]

        # In common
        in_common_header = f"In common \n{_print_num_columns(len(columns.in_common()))}"
        table.add_column(
            in_common_header,
            justify="center",
            min_width=min_width,
            max_width=SCHEMAS_COLUMN_WIDTH,
            overflow=OVERFLOW,
        )
        num_in_common = len(columns.in_common())
        table_data[in_common_header] = []
        common_but_mismatching = columns.in_common().mismatching_dtypes()
        if len(common_but_mismatching) == 0:
            table_data[in_common_header] = ["..."]
            max_column_width = max(
                max_column_width, len(table_data[in_common_header][0])
            )
        else:
            for col, (left_dtype, right_dtype) in sorted(
                common_but_mismatching.items(), key=lambda x: x[0]
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
        if len(right_only) > 0:
            right_only_header = f"{capitalize_first(self.right_name)} only\n{_print_num_columns(len(right_only))}"
            table.add_column(
                right_only_header,
                header_style="green",
                justify="center",
                min_width=min_width,
                max_width=SCHEMAS_COLUMN_WIDTH,
                overflow=OVERFLOW,
            )
            table_data[right_only_header] = [
                _format_colname(col) for col in sorted(right_only)
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
        content: RenderableType
        if self._comparison.primary_key is None:
            content = self._print_rows_without_primary_key()
            equal = self._comparison.equal_num_rows()
        else:
            content = self._print_rows_with_primary_key()
            equal = self._comparison._equal_rows()
        # NOTE: In slim mode, we only print the section if there are differences.
        if not self.slim or not equal:
            _print_section(console, "Rows", content)

    def _print_rows_without_primary_key(self) -> RenderableType:
        content: RenderableType
        if self._comparison.equal_num_rows():
            content = Text(
                "The number of rows matches exactly (row count: "
                f"{self._comparison.num_rows_left():,}).",
                style="italic",
            )
        else:
            content = self._section_row_counts()
        return content

    def _print_rows_with_primary_key(self) -> RenderableType:
        content: RenderableType
        if self._comparison._equal_rows():
            content = Text(
                f"All rows match exactly (row count: {self._comparison.num_rows_left():,}).",
                style="italic",
            )
        else:
            # NOTE: In slim mode, we omit the row counts section and only show the
            # row matches section.
            if self._comparison.equal_num_rows() and self.slim:
                content = Group(self._section_row_matches())
            else:
                content = Group(
                    self._section_row_counts(),
                    "",
                    self._section_row_matches(),
                )
        return content

    def _section_row_counts(self) -> RenderableType:
        gain_loss = ""
        if self._comparison.num_rows_left() > 0:
            fraction_rows_right = (
                self._comparison.num_rows_right() / self._comparison.num_rows_left()
            )
            if fraction_rows_right > 1:
                gain_loss = f"(+{(fraction_rows_right - 1):.2%})"
            elif fraction_rows_right < 1:
                gain_loss = f"(-{(1 - fraction_rows_right):.2%})"
            else:
                gain_loss = "(no change)"

        ### Row counts
        count_rows: list[RenderableType] = []

        count_grid = Table(padding=0, box=None)
        left_header = f"{capitalize_first(self.left_name)} count"
        right_header = f"{capitalize_first(self.right_name)} count"
        count_grid.add_column(left_header, justify="center")
        count_grid.add_column("", justify="center")
        count_grid.add_column(right_header, justify="center")
        count_grid.add_row(
            f"{self._comparison.num_rows_left():,}",
            f" {gain_loss} ",
            f"{self._comparison.num_rows_right():,}",
        )
        count_rows.append(count_grid)

        return Group(*count_rows)

    def _section_row_matches(self) -> RenderableType:
        columns: list[RenderableType] = []
        num_dummy_cols = 5

        # Left Table
        if self._comparison.num_rows_left() > 0:
            left_table = Table(show_header=False, padding=0, box=box.HEAVY_EDGE)
            for _ in range(num_dummy_cols):
                left_table.add_column()
            if self._comparison.num_rows_left_only() > 0:
                left_table.add_row(*([Text("-", style="red")] * num_dummy_cols))
                left_table.add_section()
            if self._comparison.num_rows_joined_equal() > 0:
                left_table.add_row(*([" "] * num_dummy_cols))
                left_table.add_section()
            if self._comparison.num_rows_joined_unequal() > 0:
                left_table.add_row(*([" "] * num_dummy_cols))
                left_table.add_section()

            columns.append(left_table)

        # Separator between tables
        if self._comparison.num_rows_joined() > 0:
            rows: list[RenderableType] = []
            if self._comparison.num_rows_left_only() > 0:
                rows.append("\n")
            if self._comparison.num_rows_joined_equal() > 0:
                rows.append("╌" * 3)
                rows.append(Text(" = ", style="bold"))
            if self._comparison.num_rows_joined_unequal() > 0:
                rows.append("╌" * 3)
                rows.append(Text(" ≠ ", style="bold"))
            rows.append("╌" * 3)

            columns.append(Group(*rows))
        else:
            columns.append(" " * 3)

        # Right table
        if self._comparison.num_rows_right() > 0:
            right_table = Table(show_header=False, padding=0, box=box.HEAVY_EDGE)
            for _ in range(num_dummy_cols):
                right_table.add_column()
            if self._comparison.num_rows_joined_equal() > 0:
                right_table.add_row(*([" "] * num_dummy_cols))
                right_table.add_section()
            if self._comparison.num_rows_joined_unequal() > 0:
                right_table.add_row(*([" "] * num_dummy_cols))
                right_table.add_section()
            if self._comparison.num_rows_right_only() > 0:
                right_table.add_row(*([Text("+", style="green")] * num_dummy_cols))

            if self._comparison.num_rows_left_only() > 0:
                columns.append(Group("\n", right_table))
            else:
                columns.append(right_table)

        # Numbers for groups
        if (
            self._comparison.num_rows_left() > 0
            or self._comparison.num_rows_right() > 0
        ):
            grid = Table(
                show_header=False,
                box=box.Box(
                    "\n".join(
                        (  # header row
                            ["╌" * 4]
                            if (
                                self._comparison.num_rows_left_only() == 0
                                and self._comparison.num_rows_left() > 0
                            )
                            else [" " * 4]
                        )
                        + [" " * 4] * 3
                        + ["╌" * 4]
                        + [" " * 4] * 2
                        + (  # bottom row
                            ["╌" * 4]
                            if (
                                self._comparison.num_rows_right_only() == 0
                                and self._comparison.num_rows_right() > 0
                            )
                            else [" " * 4]
                        )
                    )
                ),
                padding=(0, 0, 0, 1),
            )
            grid.add_column("Count", justify="right")
            grid.add_column("Type", justify="left")
            grid.add_column("Percentage", justify="right")
            if self._comparison.num_rows_left_only() > 0:
                fraction_left_only = (
                    self._comparison.num_rows_left_only()
                    / self._comparison.num_rows_left()
                )
                grid.add_row(
                    f"{self._comparison.num_rows_left_only():,}",
                    f"{self.left_name} only",
                    f"({_format_fraction_as_percentage(fraction_left_only)})",
                )
                grid.add_section()
            if self._comparison.num_rows_joined_equal() > 0:
                fraction_equal = (
                    self._comparison.num_rows_joined_equal()
                    / self._comparison.num_rows_joined()
                )
                grid.add_row(
                    f"{self._comparison.num_rows_joined_equal():,}",
                    "equal",
                    f"({_format_fraction_as_percentage(fraction_equal)})",
                )
                grid.add_section()
            if self._comparison.num_rows_joined_unequal() > 0:
                fraction_unequal = (
                    self._comparison.num_rows_joined_unequal()
                    / self._comparison.num_rows_joined()
                )
                grid.add_row(
                    f"{self._comparison.num_rows_joined_unequal():,}",
                    "unequal",
                    f"({_format_fraction_as_percentage(fraction_unequal)})",
                )
                grid.add_section()
            if self._comparison.num_rows_right_only() > 0:
                fraction_right_only = (
                    self._comparison.num_rows_right_only()
                    / self._comparison.num_rows_right()
                )
                grid.add_row(
                    f"{self._comparison.num_rows_right_only():,}",
                    f"{self.right_name} only",
                    f"({_format_fraction_as_percentage(fraction_right_only)})",
                )
            columns.append(grid)

        # Num joined
        num_sections = (self._comparison.num_rows_joined_equal() > 0) + (
            self._comparison.num_rows_joined_unequal() > 0
        )
        if num_sections > 0:
            joined_rows: list[RenderableType] = []
            if self._comparison.num_rows_left_only() > 0:
                joined_rows.append("\n")
            joined_rows.append("╌╮")
            joined_rows.append(" │")
            if num_sections > 1:
                joined_rows.append(
                    f"╌├╴  {self._comparison.num_rows_joined():,}  joined"
                )
                joined_rows.append(" │")
            joined_rows.append("╌╯")
            columns.append(Group(*joined_rows))

        return RichColumns(columns, padding=0)

    # -------------------------------- COLUMN MATCHES -------------------------------- #

    def _print_columns(self, console: Console) -> None:
        # NOTE: We can only compute column matches if there are primary key columns and
        # at least one joined row.
        match_rates_can_be_computed = (
            self._comparison.primary_key is not None
            and self._comparison.num_rows_joined() > 0
        )
        if match_rates_can_be_computed:
            match_rates = self._comparison.fraction_same()
            # NOTE: In slim mode, we only print the columns section if there are
            # non-primary key columns and at least one column has a match rate < 1.
            if not self.slim or (
                self._comparison._other_common_columns and min(match_rates.values()) < 1
            ):
                _print_section(
                    console,
                    "Columns",
                    self._section_columns(),
                )

    def _section_columns(self) -> RenderableType:
        display_items: list[RenderableType] = []

        if self._comparison._other_common_columns and (
            self.show_perfect_column_matches
            or (min(self._comparison.fraction_same().values()) < 1)
        ):
            matches = Table(show_header=False)
            matches.add_column(
                "Column", max_width=COLUMN_SECTION_COLUMN_WIDTH, overflow=OVERFLOW
            )
            matches.add_column("Match Rate", justify="right")
            if any(
                self.top_k_changes_by_column[col_name] > 0
                for col_name in self._comparison._other_common_columns
                if self._comparison.fraction_same()[col_name] < 1
            ):
                matches.add_column("Top Changes", justify="right")
            if self.show_perfect_column_matches:
                max_col_len = max(
                    len(col) for col in self._comparison.fraction_same().keys()
                )
            else:
                max_col_len = max(
                    len(col)
                    for col, frac in self._comparison.fraction_same().items()
                    if frac < 1
                )
            for column, match_rate in sorted(
                self._comparison.fraction_same().items(), key=lambda x: x[0]
            ):
                if self.show_perfect_column_matches or match_rate < 1:
                    columns: list[RenderableType] = [
                        Text(column, style="cyan"),
                        f"{_format_fraction_as_percentage(match_rate)}",
                    ]
                    top_k_column_changes = self.top_k_changes_by_column[column]
                    if top_k_column_changes > 0:
                        all_change_counts = self._comparison.change_counts(
                            column,
                            include_sample_primary_key=self.show_sample_primary_key_per_change,
                        )

                        top_change_counts = all_change_counts.head(top_k_column_changes)

                        change_lines = []
                        for row in top_change_counts.iter_rows(named=True):
                            line = (
                                f"{_format_value(row['left'])} -> "
                                f"{_format_value(row['right'])} ({row['count']:,}x"
                            )
                            if self.show_sample_primary_key_per_change:
                                primary_key = self._comparison.primary_key
                                assert isinstance(primary_key, list)
                                line += ", e.g. "
                                if len(primary_key) == 1:
                                    line += _format_value(
                                        row[f"sample_{primary_key[0]}"]
                                    )
                                else:
                                    line += "("
                                    line += ", ".join(
                                        [
                                            _format_value(row[f"sample_{col}"])
                                            for col in primary_key
                                        ]
                                    )
                                    line += ")"
                            line += ")"
                            change_lines.append(line)

                        if (
                            remaining_count := len(all_change_counts)
                            - top_k_column_changes
                        ) > 0:
                            change_lines.append(
                                f"(...and {remaining_count:,} {('other' if remaining_count == 1 else 'others')})"
                            )

                        text = "\n".join(change_lines)
                        columns.append(text)

                    matches.add_row(*columns)
                    if (
                        top_k_column_changes > 0
                        or max_col_len > COLUMN_SECTION_COLUMN_WIDTH
                    ):
                        matches.add_section()

            display_items.append(matches)
        elif not self._comparison._other_common_columns:
            display_items.append(
                Text("No common non-primary key columns to compare.", style="italic")
            )
        else:
            display_items.append(Text("All columns match perfectly.", style="italic"))

        return Group(*display_items)

    # ------------------------------ ROWS ONLY ONE SIDE ------------------------------ #

    def _print_sample_rows_only_one_side(self, console: Console, side: Side) -> None:
        if self._comparison.primary_key is None:
            return
        num_rows_only = (
            self._comparison.num_rows_left_only()
            if side == Side.LEFT
            else self._comparison.num_rows_right_only()
        )
        name = self.left_name if side == Side.LEFT else self.right_name
        if num_rows_only > 0 and self.sample_k_rows_only > 0:
            _print_section(
                console,
                f"Rows {name} only",
                self._section_rows_only_one_side(
                    side=side, sample_k_rows_only=self.sample_k_rows_only
                ),
            )

    def _section_rows_only_one_side(
        self, side: Side, sample_k_rows_only: int
    ) -> RenderableType:
        def _polars_to_rich_table(df: pl.DataFrame) -> Table:
            table = Table()
            columns = df.columns

            for col in columns[:MAX_DISPLAYED_COLUMNS_IN_SAMPLE_TABLES]:
                table.add_column(col, overflow="ellipsis")

            if len(columns) > MAX_DISPLAYED_COLUMNS_IN_SAMPLE_TABLES:
                table.add_column("...", style="dim")

            for row in df.iter_rows():
                added_row = [
                    str(v) for v in row[:MAX_DISPLAYED_COLUMNS_IN_SAMPLE_TABLES]
                ]
                if len(columns) > MAX_DISPLAYED_COLUMNS_IN_SAMPLE_TABLES:
                    added_row.append("...")
                table.add_row(*added_row)

            return table

        only_one_side = (
            self._comparison.left_only(lazy=True)
            if side == Side.LEFT
            else self._comparison.right_only(lazy=True)
        )
        primary_key = self._comparison.primary_key
        assert isinstance(primary_key, list)

        return _polars_to_rich_table(
            only_one_side.select(primary_key).head(sample_k_rows_only).collect()
        )


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
    return f"[yellow]{raw}[/yellow]"


def _trim_whitespaces(s: str) -> str:
    return "\n".join(line.rstrip() for line in s.splitlines())
