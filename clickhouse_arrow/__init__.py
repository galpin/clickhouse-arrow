"""
Provides a minimal client that uses the ClickHouse HTTP API and Apache Arrow.

Example usage:

>>> import clickhouse_arrow as ch
>>> client = ch.Client()
>>> client.read_table(
>>>     "select {p1:UInt8} + {p2:UInt8} as life",
>>>     params=dict(
>>>         p1=2,
>>>         p2=2,
>>>     ),
>>> )
"""
from typing import Any, Iterator
from urllib.parse import urlencode
from importlib_metadata import version


import pyarrow as pa
import urllib3


__version__ = version(__package__)


class Client:
    """
    A minimal client that uses the ClickHouse HTTP API and Apache Arrow.

    Args:
       url: (str) The host name of the server to connect to.
       user: (str) The optional username to authenticate with.
       password: (str) The optional password to authenticate with.
       pool: (PoolManager) The optional HTTP connection pool to use.
    """

    def __init__(
        self,
        url: str = "http://localhost:8123/",
        user: str = "default",
        password: str = "",
        pool: urllib3.PoolManager = None,
    ):
        self._url = build_url(
            url,
            user=user,
            password=password,
        )
        self._pool = pool or urllib3.PoolManager()

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

    def read_table(
        self,
        query: str,
        params: dict[str, Any] = None,
        settings: dict[str, Any] = None,
    ) -> pa.Table:
        """
        Execute a query and read the result using the Arrow format
        as a table.

        Args:
            query: (str) The query to execute.
            params: (dict) The optional named query parameters (bound server-side).
            settings: (dict) The optional request settings.

        Returns:
            A `pyarrow.Table` instance containing the results.

        Raises:
            ClickhouseException: When a non-success response status was received.
        """
        return pa.Table.from_batches(self.read_batches(query, params, settings))

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
        with self._execute(query, params, settings, format_="ArrowStream") as response:
            with pa.ipc.open_stream(response) as reader:
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
        columns = ", ".join(f'`{c}`' for c in data.column_names)
        query = f"INSERT INTO {table} ({columns}) FORMAT Arrow"
        params = urlencode({"query": query})
        url = f"{self._url}&{params}"
        headers = {"Content-Type": "application/octet-stream"}
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
        headers = {"Content-Type": content_type}
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


def build_url(url: str, **query) -> str:
    return f"{url}{'?' if not url.endswith('?') else ''}{urlencode(query)}"


def append_url(url: str, **query) -> str:
    return f"{url}&{urlencode(query)}"


def create_post_body(query: str, params: dict[str, Any]):
    body = {"query": query}
    if params:
        body.update({f"param_{k}": v for k, v in params.items()})
    return body


def ensure_success_status(response: urllib3.HTTPResponse):
    if response.status != 200:
        raise ClickhouseException(response.status, str(response.data))


def serialize_ipc(table: pa.Table) -> bytes:
    buffer = pa.BufferOutputStream()
    with pa.RecordBatchFileWriter(buffer, table.schema) as writer:
        writer.write(table)
    return buffer.getvalue()


__all__ = ["Client", "ClickhouseException"]
