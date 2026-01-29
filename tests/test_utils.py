# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from diffly._utils import capitalize_first


def test_capitalize_first() -> None:
    assert capitalize_first("left") == "Left"
    assert capitalize_first("Left") == "Left"
    assert capitalize_first("LEFT") == "LEFT"
    assert capitalize_first("left table") == "Left table"
    assert capitalize_first("") == ""
