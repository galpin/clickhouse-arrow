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

import collections.abc
from typing import Any, Iterator
from urllib.parse import urlencode

import pyarrow as pa
import urllib3
from importlib.metadata import version

__version__ = version(__package__)


class Client:
    """
    A minimal client that uses the ClickHouse HTTP API and Apache Arrow.

    Args:
       url: (str) The host name of the server to connect to, defaults to `http://localhost:8123/`.
       user: (str) The optional username to authenticate with, defaults to `default`.
       password: (str) The optional password to authenticate with, defaults to empty.
       pool: (PoolManager) The optional HTTP connection pool to use.
       default_settings: (dict) The optional default settings to include with every query.
    """

    def __init__(
        self,
        url: str = "http://localhost:8123/",
        user: str = "default",
        password: str = "",
        pool: urllib3.PoolManager = None,
        default_settings: dict[str, Any] = None,
    ):
        self._url = url
        self._headers = {
            "X-ClickHouse-User": user,
            "X-ClickHouse-Key": password,
        }
        self._pool = pool or urllib3.PoolManager()
        self._default_settings = default_settings

    def execute(
        self,
        query: str,
        params: dict[str, Any] = None,
        settings: dict[str, Any] = None,
    ):
        """
        Executes a raw query.

        Args:
            query: (str) The query to execute.
            params: (dict) The optional named query parameters (bound server-side).
            settings: (dict) The optional request settings.

        Returns:
            The raw response as a byte-string.

        Raises:
            ClickhouseException: When a non-success response status was received.
        """
        return self._execute(query, params, settings).data

    def open_stream(
        self,
        query: str,
        params: dict[str, Any] = None,
        settings: dict[str, Any] = None,
    ) -> pa.RecordBatchStreamReader:
        """
        Execute a query and open a stream to read the result using the ArrowStream format.

        Args:
            query: (str) The query to execute.
            params: (dict) The optional named query parameters (bound server-side).
            settings: (dict) The optional request settings.

        Returns:
            A `pa.RecordBatchStreamReader` instance that reads the response.

        Raises:
            ClickhouseException: When a non-success response status was received.
        """
        response = self._execute(query, params, settings, format_="ArrowStream")
        return pa.ipc.open_stream(response)

    def read_table(
        self,
        query: str,
        params: dict[str, Any] = None,
        settings: dict[str, Any] = None,
        schema: pa.schema = None,
    ) -> pa.Table:
        """
        Execute a query and read the result using the Arrow format
        as a table.

        Args:
            query: (str) The query to execute.
            params: (dict) The optional named query parameters (bound server-side).
            settings: (dict) The optional request settings.
            schema: (schema) The optional table schema.

        Returns:
            A `pyarrow.Table` instance containing the results.

        Raises:
            ClickhouseException: When a non-success response status was received.
        """
        batches = self.read_batches(query, params, settings)
        return pa.Table.from_batches(batches, schema)

    def read_batches(
        self,
        query: str,
        params: dict[str, Any] = None,
        settings: dict[str, Any] = None,
    ) -> Iterator[pa.RecordBatch]:
        """
        Execute a query and read the result using the ArrowStream format
        as an iterator of record batches.

        Args:
            query: (str) The query to execute.
            params: (dict) The optional named query parameters (bound server-side).
            settings: (dict) The optional request settings.

        Returns:
            An interator of `pyarrow.RecordBatch` instances containing
            the results.

        Raises:
            ClickhouseException: When a non-success response status was received.
        """
        with self.open_stream(query, params, settings) as reader:
            try:
                while True:
                    yield reader.read_next_batch()
            except StopIteration:
                pass

    def insert(self, table: str, data: pa.Table):
        """
        Inserts data into a table using the Arrow format.

        Column names must match between `table` and `data`.

        Args:
            table: (str) The table into which to insert data.
            data: (pyarrow.Table) The table of the data.

        Raises:
            ClickhouseException: When a non-success response status was received.
        """
        columns = ", ".join(f"`{c}`" for c in data.column_names)
        query = f"INSERT INTO {table} ({columns}) FORMAT Arrow"
        url = append_url(self._url, query=query)
        headers = self._headers | {"Content-Type": "application/octet-stream"}
        body = serialize_ipc(data)
        response = self._pool.urlopen(
            "POST",
            url,
            headers=headers,
            body=body,
        )
        ensure_success_status(response)

    def _execute(
        self,
        query: str,
        params: dict = None,
        settings: dict = None,
        format_: str = None,
    ):
        if format_:
            query += f" FORMAT {format_}"
        fields = create_post_body(query, params)
        body, content_type = urllib3.encode_multipart_formdata(fields)
        headers = self._headers | {"Content-Type": content_type}
        settings = self._combine_settings(settings)
        url = append_url(self._url, **settings) if settings else self._url
        response = self._pool.urlopen(
            "POST",
            url,
            body=body,
            headers=headers,
            preload_content=False,
        )
        ensure_success_status(response)
        return response

    def _combine_settings(
        self, settings: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        match (settings, self._default_settings):
            case (None, _):
                return self._default_settings
            case (_, None):
                return settings
            case (_, _):
                return self._default_settings | settings
        return None


class ClickhouseException(Exception):
    """
    Occurs when a non-success response is received from Clickhouse.

    Args:
        status: (int) The response status code.
        body: (str) The response body (this may include a message from the server).
    """

    def __init__(self, status, body):
        super().__init__(f"Unexpected HTTP response status code: {status}.")
        self.status = status
        self.body = body


def append_url(url: str, **query) -> str:
    return f"{url}?{urlencode(query)}"


def create_post_body(query: str, params: dict[str, Any]):
    body = {"query": query}
    if params:
        body.update({f"param_{k}": bind_param(v) for k, v in params.items()})
    return body


def bind_param(value: Any, quote_strings=False) -> str:
    # Inspired by clickhouse-connect.
    if value is None:
        return "NULL"
    if isinstance(value, (bool, int, float)):
        return str(value)
    if isinstance(value, str):
        return f"'{value}'" if quote_strings else value
    if isinstance(value, tuple):
        return f"({', '.join([bind_param(v, True) for v in value])})"
    if isinstance(value, dict):
        pairs = (
            bind_param(k, True) + ":" + bind_param(v, True) for k, v in value.items()
        )
        return f"{{{', '.join(pairs)}}}"
    if isinstance(value, collections.abc.Iterable):
        return f"[{', '.join([bind_param(v, True) for v in value])}]"
    return str(value)


def ensure_success_status(response: urllib3.HTTPResponse):
    if response.status != 200:
        raise ClickhouseException(response.status, str(response.data))


def serialize_ipc(table: pa.Table) -> bytes:
    buffer = pa.BufferOutputStream()
    with pa.RecordBatchFileWriter(buffer, table.schema) as writer:
        writer.write(table)
    return buffer.getvalue()


__all__ = ["Client", "ClickhouseException"]
