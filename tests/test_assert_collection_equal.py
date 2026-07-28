# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import pytest

from diffly.comparison import DataFrameComparison
from diffly.testing import (
    CollectionComparisonAssertionError,
    assert_collection_equal,
)

pytest.importorskip("dataframely", reason="requires dataframely")
import dataframely as dy
from dataframely.testing import create_collection, create_schema


class Foo(dy.Schema):
    index = dy.UInt8(primary_key=True)
    value = dy.Float64()


class Bar(dy.Schema):
    index = dy.UInt8(primary_key=True)
    value = dy.Float64()


class Baz(dy.Schema):
    index = dy.UInt8(primary_key=True)
    value = dy.Float64()


class Qux(dy.Collection):
    foo: dy.LazyFrame[Foo]
    bar: dy.LazyFrame[Bar]
    baz: dy.LazyFrame[Baz] | None


class Quux(dy.Collection):
    foo: dy.LazyFrame[Foo]
    bar: dy.LazyFrame[Bar]


@pytest.fixture
def matching() -> pl.DataFrame:
    return pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 3.0]})


@pytest.fixture
def mismatching() -> pl.DataFrame:
    return pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 4.0]})


def test_identical(matching: pl.DataFrame) -> None:
    qux = Qux.validate(
        {
            "foo": Foo.validate(matching, cast=True),
            "bar": Bar.validate(matching, cast=True),
        }
    )
    assert_collection_equal(qux, qux)


def test_different_types(matching: pl.DataFrame) -> None:
    qux = Qux.validate(
        {
            "foo": Foo.validate(matching, cast=True),
            "bar": Bar.validate(matching, cast=True),
        }
    )
    quux = Quux.validate(qux.to_dict())
    with pytest.raises(AssertionError, match="The collection definitions do not match"):
        assert_collection_equal(qux, quux)


def test_missing_member(matching: pl.DataFrame) -> None:
    qux1 = Qux.validate(
        {
            "foo": Foo.validate(matching, cast=True),
            "bar": Bar.validate(matching, cast=True),
            "baz": Baz.validate(matching, cast=True),
        }
    )
    qux2 = Qux.validate(
        {
            "foo": Foo.validate(matching, cast=True),
            "bar": Bar.validate(matching, cast=True),
        }
    )
    with pytest.raises(AssertionError, match="The collections have different members"):
        assert_collection_equal(qux1, qux2)


def test_unequal_members(matching: pl.DataFrame, mismatching: pl.DataFrame) -> None:
    qux1 = Qux.validate(
        {
            "foo": Foo.validate(matching, cast=True),
            "bar": Bar.validate(matching, cast=True),
        }
    )
    qux2 = Qux.validate(
        {
            "foo": Foo.validate(mismatching, cast=True),
            "bar": Bar.validate(mismatching, cast=True),
        }
    )
    with pytest.raises(AssertionError, match="The following members are not equal"):
        assert_collection_equal(qux1, qux2)


def test_error_exposes_comparisons(
    matching: pl.DataFrame, mismatching: pl.DataFrame
) -> None:
    # Arrange
    qux1 = Qux.validate(
        {
            "foo": Foo.validate(matching, cast=True),
            "bar": Bar.validate(matching, cast=True),
        }
    )
    qux2 = Qux.validate(
        {
            "foo": Foo.validate(mismatching, cast=True),
            "bar": Bar.validate(matching, cast=True),
        }
    )

    # Act
    with pytest.raises(CollectionComparisonAssertionError) as exc_info:
        assert_collection_equal(qux1, qux2)

    # Assert
    assert isinstance(exc_info.value, CollectionComparisonAssertionError)
    # Only the failing member is exposed, and its comparison is usable.
    comparisons = exc_info.value.comparisons
    assert set(comparisons) == {"foo"}
    assert isinstance(comparisons["foo"], DataFrameComparison)
    assert comparisons["foo"].fraction_same("value") == pytest.approx(2 / 3)


def test_no_primary_key() -> None:
    no_pk_schema = create_schema("NoPKSchema", {"a": dy.Integer(nullable=True)})
    collection = create_collection("Test", {"first": Foo, "second": no_pk_schema})
    collection = create_collection("test", {"first": Foo, "second": no_pk_schema})
    value = collection.create_empty()
    assert_collection_equal(value, value)
