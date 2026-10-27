# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import warnings as _warnings
from importlib import metadata as _metadata

try:
    __version__ = _metadata.version(__name__)
except _metadata.PackageNotFoundError as e:  # pragma: no cover
    _warnings.warn(f"Could not determine version of {__name__}\n{e!s}", stacklevel=2)
    __version__ = "unknown"


from ._exceptions import PrimaryKeyError
from .comparison import compare_frames

__all__ = ["PrimaryKeyError", "compare_frames"]
