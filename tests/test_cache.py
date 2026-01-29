# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from diffly._cache import cached_method


class Mock:
    def __init__(self, default: int):
        self.default = default
        self.execution_count = 0

    @cached_method
    def echo(self, value: int | None = None) -> int:
        self.execution_count += 1
        return value or self.default


def test_simple_cache() -> None:
    mock = Mock(1)
    for _ in range(3):
        assert mock.echo() == 1
    assert mock.execution_count == 1


def test_cache_args() -> None:
    mock = Mock(1)
    for _ in range(3):
        for i in range(1, 100):
            assert mock.echo(i) == i
    assert mock.execution_count == 99


def test_cache_args_vs_kwargs() -> None:
    mock = Mock(1)
    for _ in range(3):
        assert mock.echo(5) == 5
        assert mock.echo(value=5) == 5
    assert mock.execution_count == 2


def test_cache_different_instances() -> None:
    mock1 = Mock(1)
    mock2 = Mock(2)

    for _ in range(3):
        assert mock1.echo() == 1
        assert mock2.echo() == 2
        assert mock2.echo(5) == 5
        assert mock1.echo(5) == 5

    assert mock1.execution_count == 2
    assert mock2.execution_count == 2
