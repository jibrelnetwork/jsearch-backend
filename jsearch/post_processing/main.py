# !/usr/bin/env python
import asyncio
import logging
from functools import partial

import click

from jsearch.common import logs
from jsearch.post_processing.service import (
    post_processing,
    ACTION_LOG_EVENTS,
    ACTION_LOG_OPERATIONS
)
from jsearch.service_bus import service_bus

logger = logging.getLogger('post_processing')


@click.command()
@click.argument('action', default=ACTION_LOG_EVENTS, type=click.Choice([ACTION_LOG_EVENTS, ACTION_LOG_OPERATIONS]))
@click.option('--log-level', default='INFO', help="Log level")
@click.option('--workers', default=30, help="Count of parallel processes")
@click.option('--query-limit', default=10000)
@click.option('--wait', is_flag=True)
def main(log_level, action, workers, query_limit, wait):
    logs.configure(log_level)

    service = partial(post_processing, action, workers, query_limit, wait_new_result=wait)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(service_bus.start())
    try:
        loop.run_until_complete(service())
    except KeyboardInterrupt:
        loop.run_until_complete(service_bus.stop())


def run():
    try:
        main()
    except click.Abort:
        print('[FAIL] Abort')


if __name__ == '__main__':
    run()
