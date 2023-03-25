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


def test_insert_table(sut):
    actual = sut.read_table(
        "SELECT * FROM numbers",
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
    actual = sut.read_table("SELECT * FROM numbers LIMIT 3")
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
        SELECT ints FROM numbers
        WHERE ints = {i:Int64} AND strs = {s:String}
        LIMIT 1
        """,
        params={
            "i": 10,
            "s": "10",
        },
    )
    assert actual == expected


def test_execute_raises_when_non_success_status(sut):
    with pytest.raises(ClickhouseException) as actual:
        sut.read_table("syntax error")
    exception = actual.value
    assert "Unexpected HTTP response status code: 400." in str(exception)
    assert exception.body is not None


def test_execute(sut):
    actual = sut.execute("SELECT * FROM numbers LIMIT 1 FORMAT JSONEachRow")
    assert json.loads(actual) == {"ints": "0", "strs": "0"}


def test_execute_with_parameters(sut):
    actual = sut.execute(
        """
        SELECT * FROM numbers
        WHERE ints = {i:Int64} AND strs = {s:String}
        LIMIT 1
        FORMAT JSONEachRow
        """,
        params={
            "i": 10,
            "s": "10",
        },
    )
    assert json.loads(actual) == {"ints": "10", "strs": "10"}


def test_execute_with_settings(sut):
    actual = sut.execute(
        "SELECT ints FROM numbers FORMAT JSONEachRow",
        settings={"limit": 1},
    )
    assert json.loads(actual) == {"ints": "0"}


@pytest.fixture
def sut(client):
    init_db(client)
    yield client


def init_db(sut):
    sut.execute("DROP TABLE IF EXISTS numbers")
    sut.execute(
        """
        CREATE TABLE numbers (
            ints Int64 NULL,
            strs String NULL
        )
        ENGINE = Memory
        """,
    )
    sut.insert("numbers", NUMBERS)


NUMBERS = pa.Table.from_pydict(
    {
        "ints": list(range(100_000)),
        "strs": [str(x) for x in range(100_000)],
    }
)
