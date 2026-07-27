# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .comparison import DataFrameComparison


class PrimaryKeyError(ValueError):
    """Raised when there is an issue with the primary key."""


class ComparisonAssertionError(AssertionError):
    """Raised when a diffly assertion fails.

    In addition to the human-readable summary, this error carries the underlying
    comparison object(s), making interactive debugging straightforward. When a test
    fails and is run with ``--pdb`` (or another post-mortem debugger), the comparison
    can be accessed from the debugger prompt via the ``$_exception`` convenience
    variable::

        (Pdb) cmp = $_exception.comparison
        (Pdb) cmp.joined_unequal()
        (Pdb) cmp.fraction_same()

    Attributes:
        comparison: The comparison for :func:`~diffly.testing.assert_frame_equal`.
            ``None`` for collection comparisons.
        comparisons: A mapping from member name to comparison for the failing members
            of :func:`~diffly.testing.assert_collection_equal`. Empty for frame
            comparisons.
    """

    def __init__(
        self,
        message: str,
        *,
        comparison: DataFrameComparison | None = None,
        comparisons: dict[str, DataFrameComparison] | None = None,
    ) -> None:
        super().__init__(message)
        self.comparison = comparison
        self.comparisons = comparisons or {}
