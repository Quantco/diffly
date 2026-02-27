# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from typing import Any


class _DummyModule:  # pragma: no cover
    def __init__(self, module: str) -> None:
        self.module = module

    def __getattr__(self, name: str) -> Any:
        raise ValueError(f"Module '{self.module}' is not installed.")


# ------------------------------------ DATAFRAMELY ------------------------------------ #

try:
    import dataframely as dy
except ImportError:  # pragma: no cover
    dy = _DummyModule("dataframely")  # type: ignore

# --------------------------------------- TYPER -------------------------------------- #

try:
    import typer
except ImportError:  # pragma: no cover
    typer = _DummyModule("typer")  # type: ignore
