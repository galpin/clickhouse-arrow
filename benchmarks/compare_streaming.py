# Copyright 2023 Martin Galpin <galpin@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
"""
Streaming-vs-buffered benchmark.

Measures two things on a query that returns many Arrow record batches:

  * time-to-first-batch    -- how soon Python sees a useful batch after
                              issuing the request. The buffered path has
                              to read the whole body first; the streaming
                              path returns the first batch as soon as it
                              has arrived from the wire.
  * total wall-clock       -- how long it takes to consume the entire
                              result. Streaming should be in the same
                              ballpark as buffered (sometimes faster
                              because parsing overlaps with network).

Three transports:

  1. urllib3 (sync, streaming)        -- preload_content=False; baseline.
  2. primp (sync, buffered)           -- the current production path.
  3. ch_http_native (sync, streaming) -- the Rust prototype with the new
                                         Response.read(n) surface.

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


MODES = {
    "urllib3 (streaming)":         open_stream_urllib3,
    "primp (buffered)":            open_stream_primp_buffered,
    "native (buffered)":           open_stream_native_buffered,
    "native (streaming)":          open_stream_native_streaming,
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
