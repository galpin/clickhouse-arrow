# Copyright 2023 Martin Galpin <galpin@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

import pyarrow as pa
import pytest

from clickhouse_arrow import ClickhouseException
from .conftest import create_client


def test_insert_table(sut):
    actual = sut.read_table(
        "select * from numbers",
        settings={"output_format_arrow_string_as_string": 1},
    )
    assert actual.equals(NUMBERS)


def test_read_table(sut):
    expected = pa.Table.from_pydict(
        {
            "ints": [0, 1, 2],
            "strs": [b"0", b"1", b"2"],
        }
    )
    actual = sut.read_table("select * from numbers limit 3")
    assert actual.equals(expected)
    assert actual.schema == pa.schema(
        [
            pa.field("ints", pa.int64()),
            pa.field("strs", pa.binary()),
        ]
    )


def test_read_table_with_parameters(sut):
    expected = pa.Table.from_pydict({"ints": [10]})
    actual = sut.read_table(
        """
        select ints from numbers
        where ints = {i:Int64} and strs = {s:String}
        limit 1
        """,
        params={
            "i": 10,
            "s": "10",
        },
    )
    assert actual == expected


def test_read_table_raises_when_query_returns_no_rows(sut):
    with pytest.raises(Exception) as actual:
        sut.read_table("select * from numbers where ints < 0")
    exception = actual.value
    assert "Must pass schema, or at least one RecordBatch" in str(exception)


def test_read_table_with_schema(sut):
    expected = pa.Table.from_pydict(
        {
            "ints": [0, 1, 2],
            "strs": [b"0", b"1", b"2"],
        }
    )
    schema = pa.schema(
        [
            ("ints", pa.int64()),
            ("strs", pa.binary()),
        ]
    )
    actual = sut.read_table(
        "select * from numbers limit 3",
        schema=schema,
    )
    assert actual == expected
    assert actual.schema == schema


def test_read_table_with_schema_when_query_returns_no_rows(sut):
    schema = pa.schema(
        [
            ("ints", pa.int64()),
            ("strs", pa.binary()),
        ]
    )
    actual = sut.read_table(
        "select * from numbers where ints < 0",
        schema=schema,
    )
    assert len(actual) == 0
    assert actual.schema == schema


def test_execute_raises_when_non_success_status(sut):
    with pytest.raises(ClickhouseException) as actual:
        sut.read_table("syntax error")
    exception = actual.value
    assert "Unexpected HTTP response status code: 400." in str(exception)
    assert exception.body is not None


def test_execute(sut):
    actual = sut.execute("select strs from numbers limit 1 format JSONEachRow")
    assert json.loads(actual) == {"strs": "0"}


def test_execute_with_parameters(sut):
    actual = sut.read_table(
        """
        select * from numbers
        where ints = {i:Int64} AND strs = {s:String}
        limit 1
        """,
        params={
            "i": 10,
            "s": "10",
        },
    )
    assert_table(actual, {"ints": [10], "strs": [b"10"]})


def test_execute_with_settings(sut):
    actual = sut.execute(
        "select ints from numbers format JSONEachRow",
        settings={"limit": 1},
    )
    assert json.loads(actual) == {"ints": "0"}


def test_execute_with_default_settings(clickhouse):
    sut = create_client(clickhouse, default_settings={"limit": 1})
    actual = sut.execute("select ints from numbers format JSONEachRow")
    assert json.loads(actual) == {"ints": "0"}


def test_execute_with_default_and_query_settings(clickhouse):
    sut = create_client(clickhouse, default_settings={"limit": 42})
    actual = sut.execute(
        "select ints from numbers format JSONEachRow",
        settings={"limit": 1},
    )
    assert json.loads(actual) == {"ints": "0"}


def test_execute_with_int_parameter(sut):
    actual = sut.read_table(
        "select {expected:Int64} as actual",
        params={"expected": 10},
    )
    assert_table(actual, {"actual": [10]})


def test_execute_with_str_parameter(sut):
    actual = sut.read_table(
        "select {expected:String} as actual",
        params={"expected": "10"},
    )
    assert_table(actual, {"actual": [b"10"]})


@pytest.mark.parametrize(
    "param_type, param, expected",
    [
        [
            "Array(Int64)",
            [1, 2, 3],
            {"actual": [[1, 2, 3]]},
        ],
        [
            "Array(String)",
            ["1", "2", "3"],
            {"actual": [[b"1", b"2", b"3"]]},
        ],
    ],
)
def test_execute_with_array_parameter(sut, param_type, param, expected):
    actual = sut.read_table(
        f"select {{expected:{param_type}}} as actual",
        params={"expected": param},
    )
    assert_table(actual, expected)


def test_execute_with_tuple_parameter(sut):
    actual = sut.read_table(
        "select {expected:Tuple(Int64, Int64, Int64)} as actual",
        params={"expected": (1, 2, 3)},
    )
    assert_table(actual, {"actual": [{"1": 1, "2": 2, "3": 3}]})


def test_execute_with_null_parameter(sut):
    actual = sut.read_table(
        "select {expected:Nullable(String)} as actual",
        params={"expected": None},
    )
    assert_table(actual, {"actual": [b"NULL"]})


@pytest.mark.parametrize(
    "expected",
    [True, False],
)
def test_execute_with_bool_parameter(sut, expected):
    actual = sut.read_table(
        "select {expected:Bool} as actual",
        params={"expected": expected},
    )
    assert_table(actual, {"actual": [expected]})


@pytest.mark.parametrize(
    "param_type, param, expected",
    [
        [
            "Map(String, Int64)",
            {"a": 1, "b": 2},
            {"actual": [[(b"a", 1), (b"b", 2)]]},
        ],
        [
            "Map(Int64, String)",
            {1: "a", 2: "b"},
            {"actual": [[(1, b"a"), (2, b"b")]]},
        ],
        [
            "Map(Int64, Array(Int64))",
            {1: [1, 2, 3]},
            {"actual": [[(1, [1, 2, 3])]]},
        ],
        [
            "Map(Int64, Map(String, String))",
            {1: {"a": "b"}},
            {"actual": [[(1, [(b"a", b"b")])]]},
        ],
    ],
)
def test_execute_with_map_parameter(sut, param_type, param, expected):
    actual = sut.read_table(
        f"select {{expected:{param_type}}} as actual",
        params={"expected": param},
    )
    assert_table(actual, expected)


def assert_table(actual, expected):
    assert actual.to_pydict() == expected


@pytest.fixture
def sut(client):
    init_db(client)
    yield client


def init_db(sut):
    sut.execute("drop table if exists numbers")
    sut.execute(
        """
        create table numbers (
            ints Int64 NULL,
            strs String NULL
        )
        engine = Memory
        """,
    )
    sut.insert("numbers", NUMBERS)


NUMBERS = pa.Table.from_pydict(
    {
        "ints": list(range(100_000)),
        "strs": [str(x) for x in range(100_000)],
    }
)
