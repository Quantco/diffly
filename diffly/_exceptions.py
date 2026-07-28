# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .comparison import DataFrameComparison


class PrimaryKeyError(ValueError):
    """Raised when there is an issue with the primary key."""


class ComparisonAssertionError(AssertionError):
    """Base class for diffly assertion failures.

    The concrete subclasses carry the underlying comparison object(s), making
    interactive debugging straightforward. When a test fails and is run with ``--pdb``
    (or another post-mortem debugger), the comparison can be accessed from the debugger
    prompt via the ``$_exception`` convenience variable. Catch this base class to handle
    both frame and collection failures.
    """


class FrameComparisonAssertionError(ComparisonAssertionError):
    """Raised when :func:`~diffly.testing.assert_frame_equal` fails.

    Access the underlying comparison from a post-mortem debugger via::

        (Pdb) cmp = $_exception.comparison
        (Pdb) cmp.joined_unequal()
        (Pdb) cmp.fraction_same()

    Attributes:
        comparison: The comparison between the two data frames.
    """

    def __init__(self, message: str, *, comparison: DataFrameComparison) -> None:
        super().__init__(message)
        self.comparison = comparison


class CollectionComparisonAssertionError(ComparisonAssertionError):
    """Raised when :func:`~diffly.testing.assert_collection_equal` fails.

    Access the failing member comparisons from a post-mortem debugger via::

        (Pdb) cmps = $_exception.comparisons
        (Pdb) cmps["some_member"].joined_unequal()

    Attributes:
        comparisons: A mapping from member name to comparison for the failing members.
    """

    def __init__(
        self, message: str, *, comparisons: dict[str, DataFrameComparison]
    ) -> None:
        super().__init__(message)
        self.comparisons = comparisons
