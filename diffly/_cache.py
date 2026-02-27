# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import functools
from collections.abc import Callable
from typing import Any, Concatenate, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


def cached_method(
    fn: Callable[Concatenate[Any, P], T],
) -> Callable[Concatenate[Any, P], T]:
    """Cache all results from the executions from an instance method."""
    cache_name = f"_{fn.__name__}_cache"
    kwd_mark = object()  # sentinel for separating args from kwargs

    @functools.wraps(fn)
    def wrapped(self: Any, *args: P.args, **kwargs: P.kwargs) -> T:
        key = args + (kwd_mark,) + tuple(sorted(kwargs.items()))

        if not hasattr(self, cache_name):
            setattr(self, cache_name, {})
        if key in getattr(self, cache_name):
            return getattr(self, cache_name)[key]

        result = fn(self, *args, **kwargs)
        getattr(self, cache_name)[key] = result
        return result

    return wrapped
