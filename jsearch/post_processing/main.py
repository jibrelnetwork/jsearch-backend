# !/usr/bin/env python
import logging
from functools import partial

import click

from jsearch import settings
from jsearch.common import logs
from jsearch.post_processing.service import (
    post_processing as service,
    ACTION_LOG_EVENTS,
    ACTION_LOG_OPERATIONS
)

logger = logging.getLogger('post_processing')


@click.command()
@click.argument('action', default=ACTION_LOG_EVENTS, type=click.Choice([ACTION_LOG_EVENTS, ACTION_LOG_OPERATIONS]))
@click.option('--log-level', default='INFO', help="Log level")
@click.option('--workers', default=settings.JSEARCH_SYNC_PARALLEL, help="Count of parallel processes")
@click.option('--query-limit', default=1000)
@click.option('--wait', is_flag=True)
def post_processing(log_level, action, workers, query_limit, wait):
    logs.configure(log_level)

    async_func = partial(service, action=action, workers=workers, query_limit=query_limit, wait_new_result=wait)
    async_func()


def run():
    try:
        post_processing()
    except click.Abort:
        print('[FAIL] Abort')


if __name__ == '__main__':
    run()
