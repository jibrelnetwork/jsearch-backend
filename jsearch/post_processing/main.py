# !/usr/bin/env python
import asyncio
import logging

import click

from jsearch.common import logs
from jsearch.post_processing.service import (
    post_processing,
    ACTION_LOG_EVENTS,
    ACTION_LOG_OPERATIONS
)

logger = logging.getLogger('post_processing')


@click.command()
@click.argument('action', default=ACTION_LOG_EVENTS, type=click.Choice([ACTION_LOG_EVENTS, ACTION_LOG_OPERATIONS]))
@click.option('--log-level', default='INFO', help="Log level")
@click.option('--workers', default=30, help="Count of parallel processes")
@click.option('--query-limit', default=10000)
@click.option('--wait', is_flag=True)
def main(log_level, action, workers, query_limit, wait):
    logs.configure(log_level)

    async_func = post_processing(action=action, workers=workers, query_limit=query_limit, wait_new_result=wait)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_func)


def run():
    try:
        main()
    except click.Abort:
        print('[FAIL] Abort')


if __name__ == '__main__':
    run()
