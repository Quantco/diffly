# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import datetime as dt
import warnings
from collections.abc import Iterable, Mapping, Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Literal, Self, overload

import polars as pl
from polars.schema import Schema as PolarsSchema

from ._cache import cached_method
from ._conditions import condition_equal_columns, condition_equal_rows
from ._utils import (
    ABS_TOL_DEFAULT,
    ABS_TOL_TEMPORAL_DEFAULT,
    REL_TOL_DEFAULT,
    Side,
    get_select_columns,
    is_primary_key,
    lazy_len,
    make_and_validate_mapping,
)

if TYPE_CHECKING:  # pragma: no cover
    # NOTE: We cannot import at runtime as we're otherwise running into circular
    #  imports. We're importing again below where we need `Summary` for more than
    #  type annotations.
    from .summary import Summary


def compare_frames(
    left: pl.DataFrame | pl.LazyFrame,
    right: pl.DataFrame | pl.LazyFrame,
    /,
    *,
    primary_key: str | Sequence[str] | None = None,
    abs_tol: float | Mapping[str, float] = ABS_TOL_DEFAULT,
    rel_tol: float | Mapping[str, float] = REL_TOL_DEFAULT,
    abs_tol_temporal: dt.timedelta
    | Mapping[str, dt.timedelta] = ABS_TOL_TEMPORAL_DEFAULT,
) -> DataFrameComparison:
    """Compare two :mod:`polars` data frames.

    Args:
        left: The first data frame in the comparison.
        right: The second data frame in the comparison.
        primary_key: Primary key columns to use for joining the data frames. If not
            provided, comparisons based on joins will raise an error.
        abs_tol: Absolute tolerance for comparing floating point types. If a
            :class:`Mapping` is provided, it should map from column name to absolute
            tolerance for every column in the data frame (except the primary key).
        rel_tol: Relative tolerance for comparing floating point types. If a
            :class:`Mapping` is provided, it should map from column name to relative
            tolerance for every column in the data frame (except the primary key).
        abs_tol_temporal: Absolute tolerance for comparing temporal types. If a
            :class:`Mapping` is provided, it should map from column name to absolute
            temporal tolerance for every column in the data frame (except the primary
            key).

    Returns:
        A data frame comparison object that can be used to explore the differences of
        the provided data frames.

    Note:
        The implementation of floating point equivalence mirrors the implementation of
        :meth:`math.isclose`.
    """
    return DataFrameComparison._init_with_validation(
        left,
        right,
        primary_key=primary_key,
        abs_tol=abs_tol,
        rel_tol=rel_tol,
        abs_tol_temporal=abs_tol_temporal,
    )


class DataFrameComparison:
    """Object representing a comparison between two :mod:`polars` data frames.

    Note:
        Do not initialize this object directly. Instead, use :meth:`compare_frames`.
    """

    def __init__(
        self,
        left: pl.LazyFrame,
        right: pl.LazyFrame,
        left_schema: PolarsSchema,
        right_schema: PolarsSchema,
        primary_key: list[str] | None,
        _other_common_columns: list[str],
        abs_tol_by_column: dict[str, float],
        rel_tol_by_column: dict[str, float],
        abs_tol_temporal_by_column: dict[str, dt.timedelta],
    ) -> None:
        self.left = left
        self.right = right
        self.left_schema = left_schema
        self.right_schema = right_schema
        self.primary_key = primary_key
        self._other_common_columns = _other_common_columns
        self.abs_tol_by_column = abs_tol_by_column
        self.rel_tol_by_column = rel_tol_by_column
        self.abs_tol_temporal_by_column = abs_tol_temporal_by_column

    @classmethod
    def _init_with_validation(
        cls,
        left: pl.DataFrame | pl.LazyFrame,
        right: pl.DataFrame | pl.LazyFrame,
        primary_key: str | Sequence[str] | None,
        abs_tol: float | Mapping[str, float],
        rel_tol: float | Mapping[str, float],
        abs_tol_temporal: dt.timedelta | Mapping[str, dt.timedelta],
    ) -> Self:
        left = left.lazy()
        right = right.lazy()
        left_schema = left.collect_schema()
        right_schema = right.collect_schema()

        # Validate the primary key
        primary_key = (
            [primary_key]
            if isinstance(primary_key, str)
            else (None if primary_key is None else list(primary_key))
        )
        if primary_key is not None:
            if len(primary_key) == 0:
                raise ValueError("The primary key columns must not be an empty list.")
            if missing := (set(primary_key) - set(left_schema.names())):
                raise ValueError(
                    f"The primary key columns must be present in the left data frame, "
                    f"but the following are missing: {', '.join(missing)}."
                )
            if missing := (set(primary_key) - set(right_schema.names())):
                raise ValueError(
                    f"The primary key columns must be present in the right data frame, "
                    f"but the following are missing: {', '.join(missing)}."
                )
            if not is_primary_key(left, primary_key):
                raise ValueError(
                    "The columns are not a primary key for the left data frame."
                )
            if not is_primary_key(right, primary_key):
                raise ValueError(
                    "The columns are not a primary key for the right data frame."
                )

            # Try joining empty frames to check if the primary key columns are
            # compatible. If not, we set the primary key to `None` and emit an
            # appropriate warning.
            try:
                left_schema.to_frame().join(right_schema.to_frame(), on=primary_key)
            except pl.exceptions.SchemaError as e:
                warnings.warn(
                    "`primary_key` is set to None as the primary key of the left and "
                    "right tables have incompatible data types: "
                    + str(e).split("\n")[0],
                )
                primary_key = None

        # Assign other relevant attributes
        schemas = Schemas(left_schema, right_schema)
        if primary_key:
            _other_common_columns = sorted(
                schemas.in_common().column_names() - set(primary_key)
            )
        else:
            _other_common_columns = sorted(schemas.in_common().column_names())

        abs_tol_by_column = make_and_validate_mapping(
            value_or_mapping=abs_tol, other_common_columns=_other_common_columns
        )
        rel_tol_by_column = make_and_validate_mapping(
            value_or_mapping=rel_tol, other_common_columns=_other_common_columns
        )
        abs_tol_temporal_by_column = make_and_validate_mapping(
            value_or_mapping=abs_tol_temporal,
            other_common_columns=_other_common_columns,
        )
        return cls(
            left=left,
            right=right,
            left_schema=left_schema,
            right_schema=right_schema,
            primary_key=primary_key,
            _other_common_columns=_other_common_columns,
            abs_tol_by_column=abs_tol_by_column,
            rel_tol_by_column=rel_tol_by_column,
            abs_tol_temporal_by_column=abs_tol_temporal_by_column,
        )

    @cached_property
    def schemas(self) -> Schemas:
        """Obtain information about the schemas of each data frame."""
        return Schemas(self.left_schema, self.right_schema)

    @cached_method
    def num_rows_left(self) -> int:
        """Number of rows in the left data frame."""
        return lazy_len(self.left)

    @cached_method
    def num_rows_right(self) -> int:
        """Number of rows in the right data frame."""
        return lazy_len(self.right)

    @overload
    def joined(self, *, lazy: Literal[True]) -> pl.LazyFrame: ...

    @overload
    def joined(self, *, lazy: Literal[False] = False) -> pl.DataFrame: ...

    def joined(self, *, lazy: bool = False) -> pl.DataFrame | pl.LazyFrame:
        """The rows of both data frames that can be joined, regardless of whether column
        values match in columns which are not used for joining.

        Args:
            lazy: If ``True``, return a lazy frame. Otherwise, return an eager frame
                (default).

        Returns:
            A data frame or lazy frame containing the rows that can be joined.

        Columns which are not used for joining have a suffix ``_left`` for the left data
        frame and a suffix ``_right`` for the right data frame.
        """
        primary_key = self._check_primary_key()
        result = (
            self.left.rename(
                {col: f"{col}_{Side.LEFT}" for col in self._other_common_columns}
            )
            .join(
                self.right.rename(
                    {col: f"{col}_{Side.RIGHT}" for col in self._other_common_columns}
                ),
                on=primary_key,
                nulls_equal=True,
            )
            .select(get_select_columns(primary_key, self._other_common_columns))
        )
        return result if lazy else result.collect()

    @cached_method
    def num_rows_joined(self) -> int:
        """The number of rows that can be joined, regardless of whether column values
        match in columns which are not used for joining."""
        return lazy_len(self.joined(lazy=True))

    @overload
    def joined_equal(self, *subset: str, lazy: Literal[True]) -> pl.LazyFrame: ...

    @overload
    def joined_equal(
        self, *subset: str, lazy: Literal[False] = False
    ) -> pl.DataFrame: ...

    def joined_equal(
        self, *subset: str, lazy: bool = False
    ) -> pl.DataFrame | pl.LazyFrame:
        """The rows of both data frames that can be joined and have matching values in
        in all columns in `subset`.

        Args:
            subset: The columns to check for mismatches. If not provided, all common
                columns are used.
            lazy: If ``True``, return a lazy frame. Otherwise, return an eager frame
                (default).

        Returns:
            A data frame or lazy frame containing the rows that can be joined and have
            matching values across the specified columns.

        Raises:
            ValueError: If any of the provided columns are not common columns.

        Columns which are not used for joining have a suffix ``_left`` for the left data
        frame and a suffix ``_right`` for the right data frame.
        """
        columns = self._validate_subset_of_common_columns(subset)
        result = self.joined(lazy=True).filter(self._condition_equal_rows(columns))
        return result if lazy else result.collect()

    @cached_method
    def num_rows_joined_equal(self, *subset: str) -> int:
        """The number of rows that can be joined and have matching values in all columns
        in `subset`.

        Args:
            subset: The columns to check for mismatches. If not provided, all common
                columns are used.

        Returns:
            The number of rows that can be joined and have matching values across the
            specified columns.

        Raises:
            ValueError: If any of the provided columns are not common columns.
        """
        return lazy_len(self.joined_equal(*subset, lazy=True))

    @overload
    def joined_unequal(
        self,
        *subset: str,
        select: Literal["all", "subset"] | list[str] = "all",
        lazy: Literal[True],
    ) -> pl.LazyFrame: ...

    @overload
    def joined_unequal(
        self,
        *subset: str,
        select: Literal["all", "subset"] | list[str] = "all",
        lazy: Literal[False] = False,
    ) -> pl.DataFrame: ...

    def joined_unequal(
        self,
        *subset: str,
        select: Literal["all", "subset"] | list[str] = "all",
        lazy: bool = False,
    ) -> pl.DataFrame | pl.LazyFrame:
        """The rows of both data frames that can be joined and have at least one
        mismatching value across any column in `subset`.

        Args:
            subset: The columns to check for mismatches. If not provided, all common
                columns are used. Must only contain common columns.
            select: Which columns should be selected in the result. `"all"` (default)
                selects all columns. `"subset"` selects only the primary key and the
                columns from `subset` in the compared data frames. Providing a list of
                strings behaves the same as `"subset"` but additionally selects the
                columns in the list from the compared data frames. The list must only
                contain common columns.
            lazy: If ``True``, return a lazy frame. Otherwise, return an eager frame
                (default).

        Returns:
            A data frame or lazy frame containing the rows that can be joined and have at
            least one mismatching value across the specified columns.

        Raises:
            ValueError: If any of the provided columns are not common columns.

        Columns which are not used for joining have a suffix ``_left`` for the left data
        frame and a suffix ``_right`` for the right data frame.
        """
        if select not in ("all", "subset") and not isinstance(select, list):
            raise ValueError(
                f"Invalid value for `select`: {select}. Must be 'all', 'subset' or a "
                "list of column names."
            )

        columns_to_compare = self._validate_subset_of_common_columns(subset)
        full_result = self.joined(lazy=True).filter(
            ~self._condition_equal_rows(columns_to_compare)
        )

        if select != "all":
            primary_key = self._check_primary_key()
            # Construct the columns to select and only output the primary key and those.
            columns_to_select = columns_to_compare.copy()
            if isinstance(select, list):
                columns_to_select += self._validate_subset_of_common_columns(select)
            # Remove duplicates from columns_to_select to avoid polars errors.
            result = full_result.select(
                get_select_columns(primary_key, list(set(columns_to_select)))
            )
        else:
            result = full_result

        return result if lazy else result.collect()

    @cached_method
    def num_rows_joined_unequal(self, *subset: str) -> int:
        """The number of rows of both data frames that can be joined and have at least
        one mismatching value across any column in `subset`.

        Args:
            subset: The columns to check for mismatches. If not provided, all common
                columns are used.

        Returns:
            The number of rows that can be joined and have at least one mismatching value
            across the specified columns.

        Raises:
            ValueError: If any of the provided columns are not common columns.
        """
        return lazy_len(self.joined_unequal(*subset, lazy=True))

    @overload
    def left_only(self, *, lazy: Literal[True]) -> pl.LazyFrame: ...

    @overload
    def left_only(self, *, lazy: Literal[False] = False) -> pl.DataFrame: ...

    def left_only(self, *, lazy: bool = False) -> pl.DataFrame | pl.LazyFrame:
        """The rows in the left data frame which cannot be joined with a row in the
        right data frame.

        Args:
            lazy: If ``True``, return a lazy frame. Otherwise, return an eager frame
                (default).

        Returns:
            A data frame or lazy frame containing the rows that are only in the left data
            frame.
        """
        primary_key = self._check_primary_key()
        result = self.left.join(
            self.right, on=primary_key, how="anti", nulls_equal=True
        )
        return result if lazy else result.collect()

    @cached_method
    def num_rows_left_only(self) -> int:
        """The number of rows in the left data frame which cannot be joined with a row
        in the right data frame."""
        return lazy_len(self.left_only(lazy=True))

    @overload
    def right_only(self, *, lazy: Literal[True]) -> pl.LazyFrame: ...

    @overload
    def right_only(self, *, lazy: Literal[False] = False) -> pl.DataFrame: ...

    def right_only(self, *, lazy: bool = False) -> pl.DataFrame | pl.LazyFrame:
        """The rows in the right data frame which cannot be joined with a row in the
        left data frame.

        Args:
            lazy: If ``True``, return a lazy frame. Otherwise, return an eager frame
                (default).

        Returns:
            A data frame or lazy frame containing the rows that are only in the right data
            frame.
        """
        primary_key = self._check_primary_key()
        result = self.right.join(
            self.left, on=primary_key, how="anti", nulls_equal=True
        )
        return result if lazy else result.collect()

    @cached_method
    def num_rows_right_only(self) -> int:
        """The number of rows in the right data frame which cannot be joined with a row
        in the left data frame."""
        return lazy_len(self.right_only(lazy=True))

    @cached_method
    def equal(self, *, check_dtypes: bool = True) -> bool:
        """Whether the data frames are equal, independent of row and column order.

        Args:
            check_dtypes: Whether to check that the data types of columns match exactly.
        """
        # We explicitly check emptiness because we cannot properly sort data frames with
        # empty schemas.
        if self.left_schema.len() == 0 or self.right_schema.len() == 0:
            return self.left_schema.len() == self.right_schema.len()

        # If we have a primary key, we can use "native" diffly functionality to check
        # for equivalence.
        if self.primary_key is not None:
            return self.schemas.equal(check_dtypes=check_dtypes) and self._equal_rows()

        # Otherwise, we need to resort to a more manual check: we compare the sorted
        # data frames for exact equality.
        if not self.schemas.equal(check_dtypes=check_dtypes):
            return False

        # NOTE: We need to ensure the same column order here as, otherwise, the sorting
        #  order might differ. We also already add suffixes here since we need them
        #  below.
        common_columns = list(self.schemas.in_common().column_names())
        [left, right] = pl.collect_all(
            [
                (
                    self.left.select(
                        pl.col(common_columns).name.suffix(f"_{Side.LEFT}")
                    ).sort(by=pl.all())
                ),
                (
                    self.right.select(
                        pl.col(common_columns).name.suffix(f"_{Side.RIGHT}")
                    ).sort(by=pl.all())
                ),
            ]
        )
        if left.height != right.height:
            return False

        return (
            pl.concat([left, right], how="horizontal")
            .select(
                condition_equal_rows(
                    columns=common_columns,
                    schema_left=self.left_schema,
                    schema_right=self.right_schema,
                    abs_tol_by_column=self.abs_tol_by_column,
                    rel_tol_by_column=self.rel_tol_by_column,
                    abs_tol_temporal_by_column=self.abs_tol_temporal_by_column,
                ).all()
            )
            .item()
        )

    def equal_num_rows(self) -> bool:
        """Whether the number of rows in the left and right data frames are equal."""
        return self.num_rows_left() == self.num_rows_right()

    @overload
    def fraction_same(self, column: Literal[None] = None, /) -> dict[str, float]: ...

    @overload
    def fraction_same(self, column: str, /) -> float: ...

    @cached_method
    def fraction_same(self, column: str | None = None, /) -> float | dict[str, float]:
        """Compute the fraction of matching values.

        Args:
            column: The column to compute the fraction of matching values for. If
                ``None`` (default), compute the fraction for all columns.

        Returns:
            A single float for the fraction or a mapping from column name to fraction,
            depending on the value of ``column``. The mapping contains all common
            columns. It is empty in case there are no common (non-primary key) columns.
        """
        primary_key = self._check_primary_key()
        if column is not None:
            if column in primary_key:
                raise ValueError(
                    f"{column} is a join column for which a fraction of matching "
                    f"values cannot be computed."
                )
            if column not in self.schemas.in_common().column_names():
                raise ValueError(
                    f"{column} is not a common column, so the fraction of matching "
                    f"values cannot be computed."
                )

        if column is not None:
            value_is_same = self._condition_equal_columns(column)
            return self.joined(lazy=True).select(value_is_same.mean()).collect().item()

        result = (
            self.joined(lazy=True)
            .select(
                [
                    self._condition_equal_columns(col).mean().alias(col)
                    for col in self._other_common_columns
                ]
            )
            .collect()
        )
        if len(result) == 0:
            return dict()
        return next(result.iter_rows(named=True))

    @overload
    def change_counts(
        self,
        column: str,
        /,
        *,
        lazy: Literal[True],
        include_sample_primary_key: bool = False,
    ) -> pl.LazyFrame: ...

    @overload
    def change_counts(
        self,
        column: str,
        /,
        *,
        lazy: Literal[False] = False,
        include_sample_primary_key: bool = False,
    ) -> pl.DataFrame: ...

    def change_counts(
        self,
        column: str,
        /,
        *,
        lazy: bool = False,
        include_sample_primary_key: bool = False,
    ) -> pl.DataFrame | pl.LazyFrame:
        """Get the changes of a column, sorted in descending order of frequency.

        Args:
            column: The name of the column to compare.
            lazy: If ``True``, return a lazy frame. Otherwise, return an eager frame
                (default).
            include_sample_primary_key: Whether to include a sample primary key for each
                change.

        Returns:
            A data frame or lazy frame containing the change counts of the specified
            column, sorted by count with the most frequent change first.
        """
        result = (
            self.joined_unequal(column, lazy=True)
            .group_by(
                pl.col(f"{column}_{Side.LEFT}").alias(Side.LEFT),
                pl.col(f"{column}_{Side.RIGHT}").alias(Side.RIGHT),
            )
            .agg(
                [pl.len().alias("count")]
                + (
                    [
                        pl.col(self.primary_key or [])
                        .sort_by(self.primary_key or [])
                        .first()
                        .name.prefix("sample_")
                    ]
                    if include_sample_primary_key
                    else []
                )
            )
            .sort(by=pl.col("count", Side.LEFT, Side.RIGHT), descending=True)
        )
        return result if lazy else result.collect()

    def summary(
        self,
        show_perfect_column_matches: bool = True,
        top_k_column_changes: int = 0,
        sample_k_rows_only: int = 0,
        show_sample_primary_key_per_change: bool = False,
        left_name: str = Side.LEFT,
        right_name: str = Side.RIGHT,
        slim: bool = False,
        hidden_columns: list[str] | None = None,
    ) -> Summary:
        """Generate a summary of all aspects of the comparison.

        Args:
            show_perfect_column_matches: Whether to include column matches in the
                summary even if the column match rate is 100%. Setting this to ``False``
                is useful when comparing very wide data frames.
            top_k_column_changes: The maximum number of column values changes to
                display for columns with a match rate below 100% in the summary. When
                enabling this feature, make sure that no sensitive data is leaked.
            sample_k_rows_only: The number of rows to show in the "Rows left/right only"
                section of the summary. If 0 (default), no rows are shown. Only the
                primary key will be printed. An error will be raised if a positive
                number is provided and any of the primary key columns is also in
                `hidden_columns`.
            show_sample_primary_key_per_change: Whether to show a sample primary key per
                column change in the summary. If False (default), no primary key values
                are shown. A sample primary key can only be shown if
                `top_k_column_changes` is greater than 0, as each sample primary key is
                linked to a specific column change. An error will be raised if True and
                any of the primary key columns is also in `hidden_columns`."
            left_name: Custom display name for the left data frame.
            right_name: Custom display name for the right data frame.
            slim: Whether to generate a slim summary. In slim mode, the summary is as
                concise as possible, only showing sections that contain differences.
                As the structure of the summary can vary, it should only be used by
                advanced users who are familiar with the summary format.
            hidden_columns: Columns for which no values are printed, e.g. because they
                contain sensitive information.

        Returns:
            A summary which can be printed or written to a file.
        """
        # NOTE: We're importing here to prevent circular imports
        from .summary import Summary

        return Summary(
            self,
            show_perfect_column_matches=show_perfect_column_matches,
            top_k_column_changes=top_k_column_changes,
            sample_k_rows_only=sample_k_rows_only,
            show_sample_primary_key_per_change=show_sample_primary_key_per_change,
            left_name=left_name,
            right_name=right_name,
            slim=slim,
            hidden_columns=hidden_columns,
        )

    # ----------------------------------- UTILITIES ----------------------------------- #

    def _check_primary_key(self) -> list[str]:
        if self.primary_key is None:
            raise ValueError(
                "`primary_key` must be provided to join `left` and `right`."
            )
        return self.primary_key

    def _validate_subset_of_common_columns(self, subset: Iterable[str]) -> list[str]:
        if not subset:
            return self._other_common_columns
        if difference := set(subset) - set(self._other_common_columns):
            raise ValueError(f"{difference} are not common columns.")
        return list(subset)

    def _condition_equal_rows(self, columns: list[str]) -> pl.Expr:
        return condition_equal_rows(
            columns=columns,
            schema_left=self.left_schema,
            schema_right=self.right_schema,
            abs_tol_by_column=self.abs_tol_by_column,
            rel_tol_by_column=self.rel_tol_by_column,
            abs_tol_temporal_by_column=self.abs_tol_temporal_by_column,
        )

    def _condition_equal_columns(self, column: str) -> pl.Expr:
        return condition_equal_columns(
            column=column,
            dtype_left=self.left_schema[column],
            dtype_right=self.right_schema[column],
            abs_tol=self.abs_tol_by_column[column],
            rel_tol=self.rel_tol_by_column[column],
            abs_tol_temporal=self.abs_tol_temporal_by_column[column],
        )

    def _equal_rows(self) -> bool:
        return (
            self.num_rows_joined_equal()
            == self.num_rows_left()
            == self.num_rows_right()
        )


class Schemas:
    """Container object providing information about the schemas of compared data
    frames."""

    class Schema(dict[str, pl.DataType]):
        """Child container for the schema of a data frame."""

        def column_names(self) -> set[str]:
            """The names of the columns."""
            return set(self)

        def __and__(self, other: Self) -> Schemas.JointSchema:
            common = self.keys() & other.keys()
            return Schemas.JointSchema({k: (self[k], other[k]) for k in common})

        def __sub__(self, other: Self) -> Self:
            return self.__class__({k: v for k, v in self.items() if k not in other})

    class JointSchema(dict[str, tuple[pl.DataType, pl.DataType]]):
        """Child container for the joint schema of two data frames."""

        def column_names(self) -> set[str]:
            """The names of the columns."""
            return set(self)

        def matching_dtypes(self) -> Schemas.Schema:
            """The columns that have matching dtypes, mapped to the common dtype."""
            return Schemas.Schema({k: v[0] for k, v in self.items() if v[0] == v[1]})

        def mismatching_dtypes(self) -> Self:
            """The columns that have mismatching dtypes, mapped to the dtypes in the
            left and right data frame."""
            return self.__class__({k: v for k, v in self.items() if v[0] != v[1]})

    def __init__(
        self,
        left_schema: dict[str, pl.DataType],
        right_schema: dict[str, pl.DataType],
    ):
        self._left_schema = left_schema
        self._right_schema = right_schema

    def left(self) -> Schema:
        """Schema of the left data frame."""
        return Schemas.Schema(dict(self._left_schema))

    def right(self) -> Schema:
        """Schema of the right data frame."""
        return Schemas.Schema(dict(self._right_schema))

    def equal(self, *, check_dtypes: bool = True) -> bool:
        """Whether the schemas of the left and right data frames are equal.

        Args:
            check_dtypes: Whether to check that the data types of columns match exactly.
        """
        if check_dtypes:
            return self.left() == self.right()
        return self.left().column_names() == self.right().column_names()

    def in_common(self) -> JointSchema:
        """Columns that are present in both data frames, mapped to their data types in
        the left and right data frame."""
        return self.left() & self.right()

    def left_only(self) -> Schema:
        """Columns that are only present in the left data frame, mapped to their data
        types."""
        return self.left() - self.right()

    def right_only(self) -> Schema:
        """Columns that are only present in the right data frame, mapped to their data
        types."""
        return self.right() - self.left()
