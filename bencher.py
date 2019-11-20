#!/usr/bin/env python3
import asyncio
import os

import sys

import time
import statistics
import click
import yaml
from typing import Any, Dict, List, NamedTuple

from aiopg import sa


async def bench_once(db: sa.Engine, query: str) -> int:
    started_at = time.perf_counter_ns()

    async with db.acquire() as conn:
        await conn.execute(query)

    ended_at = time.perf_counter_ns()

    return ended_at - started_at


async def bench(dsn: str, queries: List[str], times: int) -> None:
    db = await sa.create_engine(dsn)

    coros = []

    for query in queries:
        coros.extend(bench_once(db, query) for __ in range(times))

    results: List[int] = await asyncio.gather(*coros)

    sys.stdout.write('========== RESULTS ==========' + os.linesep)
    sys.stdout.write(f'- min: {min(results) // 1000000} ms' + os.linesep)
    sys.stdout.write(f'- max: {max(results) // 1000000} ms' + os.linesep)
    sys.stdout.write(f'- mean: {statistics.mean(results) // 1000000} ms' + os.linesep)
    sys.stdout.write(f'- median: {statistics.median(results) // 1000000} ms' + os.linesep)
    sys.stdout.write(f'- total: {sum(results) // 1000000} ms' + os.linesep)
    sys.stdout.write(f'- all: {", ".join(map(lambda r: str(r // 1000000), results))} ms' + os.linesep)

    db.close()
    await db.wait_closed()


@click.command()
@click.argument('dsn', type=str)  # Target DB's DSN
@click.argument('data-filename', type=click.Path(exists=True))  # YAML with a query to benchmark and it's data
@click.option('--times', type=int, default=50, help="Times to run a query")
# TODO: pool sizes
def main(dsn: str, data_filename: str, times: int) -> None:
    with open(data_filename) as data:
        queries_raw: List[Dict[str, Any]] = yaml.safe_load(data)

    queries = []

    for q in queries_raw:
        for q_params in q['params']:
            query = q['query'].format(**q_params)
            queries.append(query)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(bench(dsn, queries, times))


if __name__ == '__main__':
    main()
