"""
Provides a minimal client that uses the ClickHouse HTTP API and Apache Arrow.

Example usage:

>>> import clickhouse_arrow as ch
>>> client = ch.Client()
>>> client.read_table(
>>>     "select {p1:UInt8} + {p2:UInt8} as life",
>>>     p1=2,
>>>     p2=2,
>>> )

"""

from typing import Any, Iterator
from urllib.parse import urlencode

import pyarrow as pa
import urllib3


class Client:
    """
    A minimal client that uses the ClickHouse HTTP API and Apache Arrow.

    Args:
       url: (str) The host name of the server to connect to.
       user: (str) The username to authenticate with.
       password: (str) The password to authenticate with.
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

    def read_table(self, query: str, **params) -> pa.Table:
        """
        Execute a query and read the result using the Arrow format
        as a table.

        Args:
            query: (str) The query to execute.
            params: (dict) The optional named query parameters (bound server-side).

        Returns:
            A `pyarrow.Table` instance containing the results.
        """
        with self._execute(query, params, format_="Arrow") as response:
            reader = pa.ipc.open_file(response.read())
            return reader.read_all()

    def read_batches(self, query: str, **params) -> Iterator[pa.RecordBatch]:
        """
        Execute a query and read the result using the ArrowStream format
        as an iterator of record batches.

        Args:
            query: (str) The query to execute.
            params: (dict) The optional named query parameters (bound server-side).

        Returns:
            An interator of `pyarrow.RecordBatch` instances containing
            the results.
        """
        with self._execute(query, params, format_="ArrowStream") as response:
            with pa.ipc.open_stream(response) as reader:
                try:
                    while True:
                        yield reader.read_next_batch()
                except StopIteration:
                    pass

    def _execute(self, query: str, params: dict, format_: str):
        query += f" FORMAT {format_}"
        fields = create_post_body(query, params)
        body, content_type = urllib3.encode_multipart_formdata(fields)
        headers = {"Content-Type": content_type}
        response = self._pool.urlopen(
            "POST",
            self._url,
            body=body,
            headers=headers,
            preload_content=False,
        )
        ensure_success_status(response)
        return response

    def insert(self, table: str, data: pa.Table):
        """
        Inserts data into a table using the Arrow format.

        Column names must match between `table` and `data`.

        Args:
            table: (str) The table into which to insert data.
            data: (pyarrow.Table) The table of the data.

        Raises:
            Exception: The HTTP response status did not indicate success.
        """
        columns = data.column_names
        columns = f" ({', '.join(c for c in columns)})"
        query = f"INSERT INTO {table}{columns} FORMAT Arrow"
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


def create_post_body(query: str, params: dict[str, Any]):
    body = {"query": query}
    if params:
        body.update({f"param_{k}": v for k, v in params.items()})
    return body


def ensure_success_status(response: urllib3.HTTPResponse):
    if response.status != 200:
        raise Exception(f"Unexepcted HTTP status code: {response.status}")


def build_url(url: str, **querystring) -> str:
    if not url.endswith("?"):
        url += "?"
    url += urlencode(querystring)
    return url


def serialize_ipc(table: pa.Table) -> bytes:
    buffer = pa.BufferOutputStream()
    with pa.RecordBatchFileWriter(buffer, table.schema) as writer:
        writer.write(table)
    return buffer.getvalue()


__all__ = ["Client"]
