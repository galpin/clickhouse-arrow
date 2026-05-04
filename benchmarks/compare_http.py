# Copyright 2023 Martin Galpin <galpin@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
"""
Benchmark comparing the old urllib3-based transport against the new
Rust-backed transports (primp and rnet) when issuing many ClickHouse HTTP
requests.

Modes:

  Sync (single-threaded, sequential)
    1. urllib3 (sync)              -- blocking baseline.
    2. rnet (sync, BlockingClient) -- raw per-request speedup.
    3. primp (sync)                -- raw per-request speedup.

  Asyncio concurrent (single thread, N coroutines in flight)
    4. rnet (async, concurrent)    -- one thread + asyncio + rnet.Client.
    5. primp (async, concurrent)   -- one thread + asyncio + primp.AsyncClient.

  Thread-pool concurrent (N OS threads, each making blocking calls)
    6. urllib3 (threads)           -- shared PoolManager; this is the model
                                      for a FlightRPC server where each
                                      gRPC worker thread blocks on I/O.
    7. rnet (sync, threads)        -- shared rnet.BlockingClient.
    8. primp (sync, threads)       -- shared primp.Client.

  The thread-pool tests answer two questions that asyncio cannot:
    * is the client thread-safe under shared use, and
    * does it release the GIL during socket I/O so threads actually run
      in parallel rather than serialising on the interpreter lock.

Run:

    docker run -d --name clickhouse-test -p 8123:8123 \
        -e CLICKHOUSE_PASSWORD=test clickhouse/clickhouse-server:23.8
    python benchmarks/compare_http.py
"""

import asyncio
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

from clickhouse_arrow import _native as ch_http_native
import primp
import rnet
import urllib3

HEADER_LIST = [("X-ClickHouse-User", "default"), ("X-ClickHouse-Key", "test")]


URL = "http://localhost:8123/"
USER = "default"
PASSWORD = "test"
HEADERS = {"X-ClickHouse-User": USER, "X-ClickHouse-Key": PASSWORD}

# A representative small query plus a larger one so we can see how each
# transport behaves when bytes-on-the-wire dominates vs. when round-trip
# overhead dominates.
QUERIES = {
    "tiny (SELECT 1)": "SELECT 1 FORMAT JSONEachRow",
    "10k rows": "SELECT number, toString(number) FROM numbers(10000) FORMAT JSONEachRow",
}

REQUESTS = 200            # total requests per run
CONCURRENCY = 32          # in-flight requests for the async run
WARMUP = 20
RUNS = 3


def bench_urllib3(query: str) -> tuple[float, int]:
    pool = urllib3.PoolManager(maxsize=1)
    url = f"{URL}?"
    total_bytes = 0
    # warmup
    for _ in range(WARMUP):
        pool.urlopen("POST", url, body=query.encode(), headers=HEADERS).data
    t0 = time.perf_counter()
    for _ in range(REQUESTS):
        r = pool.urlopen("POST", url, body=query.encode(), headers=HEADERS)
        total_bytes += len(r.data)
    return time.perf_counter() - t0, total_bytes


def bench_rnet_sync(query: str) -> tuple[float, int]:
    client = rnet.BlockingClient()
    body = query.encode()
    total_bytes = 0
    for _ in range(WARMUP):
        client.post(URL, headers=HEADERS, body=body).bytes()
    t0 = time.perf_counter()
    for _ in range(REQUESTS):
        r = client.post(URL, headers=HEADERS, body=body)
        total_bytes += len(r.bytes())
    return time.perf_counter() - t0, total_bytes


async def _rnet_async_one(client, semaphore, body):
    async with semaphore:
        r = await client.post(URL, headers=HEADERS, body=body)
        return len(await r.bytes())


async def _bench_rnet_async(query: str) -> tuple[float, int]:
    client = rnet.Client()
    body = query.encode()
    semaphore = asyncio.Semaphore(CONCURRENCY)
    # warmup
    await asyncio.gather(*[_rnet_async_one(client, semaphore, body) for _ in range(WARMUP)])
    t0 = time.perf_counter()
    sizes = await asyncio.gather(
        *[_rnet_async_one(client, semaphore, body) for _ in range(REQUESTS)]
    )
    return time.perf_counter() - t0, sum(sizes)


def bench_rnet_async(query: str) -> tuple[float, int]:
    return asyncio.run(_bench_rnet_async(query))


def bench_primp_sync(query: str) -> tuple[float, int]:
    client = primp.Client()
    body = query.encode()
    total_bytes = 0
    for _ in range(WARMUP):
        client.post(URL, headers=HEADERS, content=body).read()
    t0 = time.perf_counter()
    for _ in range(REQUESTS):
        r = client.post(URL, headers=HEADERS, content=body)
        total_bytes += len(r.read())
    return time.perf_counter() - t0, total_bytes


async def _primp_async_one(client, semaphore, body):
    async with semaphore:
        r = await client.post(URL, headers=HEADERS, content=body)
        return len(await r.aread())


async def _bench_primp_async(query: str) -> tuple[float, int]:
    client = primp.AsyncClient()
    body = query.encode()
    semaphore = asyncio.Semaphore(CONCURRENCY)
    await asyncio.gather(*[_primp_async_one(client, semaphore, body) for _ in range(WARMUP)])
    t0 = time.perf_counter()
    sizes = await asyncio.gather(
        *[_primp_async_one(client, semaphore, body) for _ in range(REQUESTS)]
    )
    return time.perf_counter() - t0, sum(sizes)


def bench_primp_async(query: str) -> tuple[float, int]:
    return asyncio.run(_bench_primp_async(query))


# ---------------------------------------------------------------------------
# Thread-pool benchmarks: N worker threads share one client. Models a Flight
# RPC server where each gRPC worker thread blocks on the HTTP call.
# ---------------------------------------------------------------------------

def _threaded(make_call):
    def run(query: str) -> tuple[float, int]:
        body = query.encode()
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
            list(pool.map(lambda _: make_call(body), range(WARMUP)))
            t0 = time.perf_counter()
            sizes = list(pool.map(lambda _: make_call(body), range(REQUESTS)))
        return time.perf_counter() - t0, sum(sizes)
    return run


def bench_urllib3_threads(query: str) -> tuple[float, int]:
    pool = urllib3.PoolManager(maxsize=CONCURRENCY)
    def call(body):
        return len(pool.urlopen("POST", URL, body=body, headers=HEADERS).data)
    return _threaded(call)(query)


def bench_rnet_sync_threads(query: str) -> tuple[float, int]:
    client = rnet.BlockingClient()
    def call(body):
        return len(client.post(URL, headers=HEADERS, body=body).bytes())
    return _threaded(call)(query)


def bench_primp_sync_threads(query: str) -> tuple[float, int]:
    client = primp.Client()
    def call(body):
        return len(client.post(URL, headers=HEADERS, content=body).read())
    return _threaded(call)(query)


# ---------------------------------------------------------------------------
# Native (ureq + PyO3) bench: a hand-rolled Rust HTTP client with the
# narrowest possible surface (POST bytes, return bytes). Sync only.
# ---------------------------------------------------------------------------

def bench_native_sync(query: str) -> tuple[float, int]:
    client = ch_http_native.Client()
    body = query.encode()
    total_bytes = 0
    for _ in range(WARMUP):
        client.post(URL, HEADER_LIST, body)
    t0 = time.perf_counter()
    for _ in range(REQUESTS):
        _, b = client.post(URL, HEADER_LIST, body)
        total_bytes += len(b)
    return time.perf_counter() - t0, total_bytes


def bench_native_threads(query: str) -> tuple[float, int]:
    client = ch_http_native.Client()
    def call(body):
        _, b = client.post(URL, HEADER_LIST, body)
        return len(b)
    return _threaded(call)(query)


def fmt_row(label: str, elapsed: float, total_bytes: int) -> str:
    rps = REQUESTS / elapsed
    mb = total_bytes / (1024 * 1024)
    return f"  {label:<22} {elapsed*1000:>8.1f} ms total  {rps:>8.1f} req/s  {mb:>7.2f} MB"


def run_one(name: str, fn, query: str) -> list[float]:
    times = []
    last_bytes = 0
    for _ in range(RUNS):
        elapsed, total_bytes = fn(query)
        times.append(elapsed)
        last_bytes = total_bytes
    best = min(times)
    median = statistics.median(times)
    print(fmt_row(f"{name} (best)", best, last_bytes))
    print(fmt_row(f"{name} (median)", median, last_bytes))
    return times


def main():
    print(f"=== Single-threaded HTTP transport benchmark ===")
    print(f"requests/run = {REQUESTS}, runs = {RUNS}, async concurrency = {CONCURRENCY}\n")

    summary = {}
    for label, query in QUERIES.items():
        print(f"--- query: {label} ---")
        urllib3_times = run_one("urllib3 (sync)", bench_urllib3, query)
        rnet_sync_times = run_one("rnet (sync)", bench_rnet_sync, query)
        primp_sync_times = run_one("primp (sync)", bench_primp_sync, query)
        native_sync_times = run_one("native (sync)", bench_native_sync, query)
        rnet_async_times = run_one("rnet (async, conc)", bench_rnet_async, query)
        primp_async_times = run_one("primp (async, conc)", bench_primp_async, query)
        urllib3_thr_times = run_one("urllib3 (threads)", bench_urllib3_threads, query)
        rnet_thr_times = run_one("rnet (threads)", bench_rnet_sync_threads, query)
        primp_thr_times = run_one("primp (threads)", bench_primp_sync_threads, query)
        native_thr_times = run_one("native (threads)", bench_native_threads, query)
        print()
        summary[label] = {
            "urllib3": min(urllib3_times),
            "rnet_sync": min(rnet_sync_times),
            "primp_sync": min(primp_sync_times),
            "native_sync": min(native_sync_times),
            "rnet_async": min(rnet_async_times),
            "primp_async": min(primp_async_times),
            "urllib3_threads": min(urllib3_thr_times),
            "rnet_threads": min(rnet_thr_times),
            "primp_threads": min(primp_thr_times),
            "native_threads": min(native_thr_times),
        }

    print("=== Speedup vs urllib3 sync (best of run, higher is better) ===")
    for label, m in summary.items():
        print(f"{label}:")
        base = m["urllib3"]
        print(f"  rnet sync       : {base/m['rnet_sync']:.2f}x")
        print(f"  primp sync      : {base/m['primp_sync']:.2f}x")
        print(f"  native sync     : {base/m['native_sync']:.2f}x")
        print(f"  rnet async      : {base/m['rnet_async']:.2f}x")
        print(f"  primp async     : {base/m['primp_async']:.2f}x")
        print(f"  urllib3 threads : {base/m['urllib3_threads']:.2f}x")
        print(f"  rnet threads    : {base/m['rnet_threads']:.2f}x")
        print(f"  primp threads   : {base/m['primp_threads']:.2f}x")
        print(f"  native threads  : {base/m['native_threads']:.2f}x")


if __name__ == "__main__":
    main()
