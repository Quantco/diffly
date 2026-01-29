# Copyright (c) QuantCo 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import pytest

from diffly.testing import assert_collection_equal

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


def test_identical() -> None:
    qux = Qux.validate(
        {
            "foo": Foo.validate(
                pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 3.0]}), cast=True
            ),
            "bar": Bar.validate(
                pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 3.0]}), cast=True
            ),
        }
    )
    assert_collection_equal(qux, qux)


def test_different_types() -> None:
    qux = Qux.validate(
        {
            "foo": Foo.validate(
                pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 3.0]}), cast=True
            ),
            "bar": Bar.validate(
                pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 3.0]}), cast=True
            ),
        }
    )
    quux = Quux.validate(qux.to_dict())
    with pytest.raises(AssertionError, match="The collection definitions do not match"):
        assert_collection_equal(qux, quux)


def test_missing_member() -> None:
    qux1 = Qux.validate(
        {
            "foo": Foo.validate(
                pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 3.0]}), cast=True
            ),
            "bar": Bar.validate(
                pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 3.0]}), cast=True
            ),
            "baz": Baz.validate(
                pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 3.0]}), cast=True
            ),
        }
    )
    qux2 = Qux.validate(
        {
            "foo": Foo.validate(
                pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 3.0]}), cast=True
            ),
            "bar": Bar.validate(
                pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 3.0]}), cast=True
            ),
        }
    )
    with pytest.raises(AssertionError, match="The collections have different members"):
        assert_collection_equal(qux1, qux2)


def test_unequal_members() -> None:
    qux1 = Qux.validate(
        {
            "foo": Foo.validate(
                pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 3.0]}), cast=True
            ),
            "bar": Bar.validate(
                pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 3.0]}), cast=True
            ),
        }
    )
    qux2 = Qux.validate(
        {
            "foo": Foo.validate(
                pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 4.0]}), cast=True
            ),
            "bar": Bar.validate(
                pl.DataFrame({"index": [1, 2, 3], "value": [1.0, 2.0, 4.0]}), cast=True
            ),
        }
    )
    with pytest.raises(AssertionError, match="The following members are not equal"):
        assert_collection_equal(qux1, qux2)


def test_no_primary_key() -> None:
    no_pk_schema = create_schema("NoPKSchema", {"a": dy.Integer(nullable=True)})
    collection = create_collection("Test", {"first": Foo, "second": no_pk_schema})
    collection = create_collection("test", {"first": Foo, "second": no_pk_schema})
    value = collection.create_empty()
    assert_collection_equal(value, value)
