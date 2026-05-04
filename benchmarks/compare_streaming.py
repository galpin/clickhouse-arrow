# Copyright 2023 Martin Galpin <galpin@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
"""
Streaming-vs-buffered-vs-zero-copy benchmark.

Measures three things on a query that returns many Arrow record batches:

  * time-to-first-batch    -- how soon Python sees a useful batch.
  * total wall-clock       -- end-to-end time to consume the result.
  * compression effect     -- whether zstd wire compression helps.

Modes:

  1. urllib3 (streaming)        -- preload_content=False; baseline.
  2. primp (buffered)           -- the previous production path.
  3. native (buffered)          -- ch_http_native, .post(), bytes -> pyarrow.
  4. native (streaming)         -- ch_http_native, .post_streaming(),
                                   pyarrow consumes via .read(n) -- still
                                   pays for PyBytes round trips.
  5. native (arrow C stream)    -- ch_http_native, .post_arrow_stream();
                                   IPC parsed in Rust, pyarrow gets batches
                                   via the C Data Interface (zero-copy).
  6. native (arrow C stream + zstd HTTP compression)

Run:

    docker run -d --name clickhouse-test -p 8123:8123 \
        -e CLICKHOUSE_PASSWORD=test clickhouse/clickhouse-server:23.8
    python benchmarks/compare_streaming.py
"""

import statistics
import time

import ch_http_native
import primp
import pyarrow as pa
import urllib3

URL = "http://localhost:8123/"
HEADERS_DICT = {"X-ClickHouse-User": "default", "X-ClickHouse-Key": "test"}
HEADERS_LIST = list(HEADERS_DICT.items())

# A query that ClickHouse will emit as many Arrow blocks. max_block_size
# forces small blocks so the difference between buffered and streaming is
# visible. ~5M rows of (Int64, String) at 1k rows/block -> ~5000 batches.
QUERY = (
    "SELECT number, toString(number) AS s "
    "FROM numbers(5000000) "
    "SETTINGS max_block_size=1000 "
    "FORMAT ArrowStream"
).encode()

WARMUP = 1
RUNS = 5


def _bench(open_stream):
    """Return (time_to_first_batch_ms, total_ms, rows, batches)."""
    t0 = time.perf_counter()
    reader = open_stream()
    batch = reader.read_next_batch()
    t_first = time.perf_counter() - t0
    rows = batch.num_rows
    batches = 1
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        rows += batch.num_rows
        batches += 1
    t_total = time.perf_counter() - t0
    return t_first * 1000, t_total * 1000, rows, batches


def open_stream_urllib3():
    pool = urllib3.PoolManager(maxsize=1)
    response = pool.urlopen(
        "POST", URL, body=QUERY, headers=HEADERS_DICT, preload_content=False
    )
    if response.status != 200:
        raise RuntimeError(response.data)
    return pa.ipc.open_stream(response)


def open_stream_primp_buffered():
    client = primp.Client()
    response = client.post(URL, headers=HEADERS_DICT, content=QUERY)
    if response.status_code != 200:
        raise RuntimeError(response.read())
    return pa.ipc.open_stream(response.read())


def open_stream_native_streaming():
    client = ch_http_native.Client()
    response = client.post_streaming(URL, HEADERS_LIST, QUERY)
    if response.status_code != 200:
        raise RuntimeError(response.read())
    return pa.ipc.open_stream(response)


def open_stream_native_buffered():
    client = ch_http_native.Client()
    status, body = client.post(URL, HEADERS_LIST, QUERY)
    if status != 200:
        raise RuntimeError(body)
    return pa.ipc.open_stream(body)


def open_stream_native_arrow_c_stream():
    client = ch_http_native.Client()
    stream = client.post_arrow_stream(URL, HEADERS_LIST, QUERY)
    return pa.RecordBatchReader.from_stream(stream)


COMPRESSED_QUERY = (
    "SELECT number, toString(number) AS s "
    "FROM numbers(5000000) "
    "SETTINGS max_block_size=1000 "
    "FORMAT ArrowStream"
).encode()
COMPRESSED_URL = URL + "?enable_http_compression=1"
COMPRESSED_HEADERS = HEADERS_LIST + [("Accept-Encoding", "zstd")]


def open_stream_native_arrow_c_stream_zstd():
    client = ch_http_native.Client()
    stream = client.post_arrow_stream(COMPRESSED_URL, COMPRESSED_HEADERS, COMPRESSED_QUERY)
    return pa.RecordBatchReader.from_stream(stream)


MODES = {
    "urllib3 (streaming)":          open_stream_urllib3,
    "primp (buffered)":             open_stream_primp_buffered,
    "native (buffered)":            open_stream_native_buffered,
    "native (streaming)":           open_stream_native_streaming,
    "native (arrow C stream)":      open_stream_native_arrow_c_stream,
    "native (arrow C + zstd HTTP)": open_stream_native_arrow_c_stream_zstd,
}


def main():
    print("=== Streaming vs buffered: time-to-first-batch ===")
    print(f"query: 5M rows of (Int64, String), max_block_size=1000\n")

    for name, fn in MODES.items():
        # warm up
        for _ in range(WARMUP):
            _bench(fn)
        firsts = []
        totals = []
        rows = batches = 0
        for _ in range(RUNS):
            tf, tt, rows, batches = _bench(fn)
            firsts.append(tf)
            totals.append(tt)
        print(
            f"  {name:<22} "
            f"first batch: {min(firsts):>7.1f} ms (median {statistics.median(firsts):>7.1f})  "
            f"total: {min(totals):>7.0f} ms (median {statistics.median(totals):>7.0f})  "
            f"rows={rows} batches={batches}"
        )


if __name__ == "__main__":
    main()
