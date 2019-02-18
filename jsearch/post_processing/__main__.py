# !/usr/bin/env python
import asyncio
import logging
from functools import partial

import click

from jsearch.common import logs
from jsearch.multiprocessing import executor
from jsearch.post_processing.reprocessing import send_erc20_transfers_to_reprocess, send_trx_logs_to_reprocess
from jsearch.post_processing.worker_logs import handle_transaction_logs
from jsearch.post_processing.worker_transfers import handle_new_transfers
from jsearch.service_bus import service_bus, ROUTE_HANDLE_TRANSACTION_LOGS, ROUTE_HANDLE_ERC20_TRANSFERS
from jsearch.utils import parse_range

logger = logging.getLogger('post_processing')

ACTION_PROCESS_LOGS = 'logs'
ACTION_PROCESS_TRANSFERS = 'transfers'

WORKER_MAP = {
    ROUTE_HANDLE_TRANSACTION_LOGS: ACTION_PROCESS_LOGS,
    ROUTE_HANDLE_ERC20_TRANSFERS: ACTION_PROCESS_TRANSFERS,
}
REPROCESSING_MAP = {
    ACTION_PROCESS_LOGS: send_erc20_transfers_to_reprocess,
    ACTION_PROCESS_TRANSFERS: send_trx_logs_to_reprocess
}

WORKERS = [handle_new_transfers, handle_transaction_logs]


async def run_service_bus():
    await service_bus.start()
    await service_bus.wait()


async def run_reprocessing(reprocessing):
    await service_bus.start()
    await reprocessing()
    await service_bus.stop()


@click.command()
@click.argument('action', type=click.Choice([ACTION_PROCESS_LOGS, ACTION_PROCESS_TRANSFERS]))
@click.option('--log-level', default='INFO', help="Log level")
@click.option('--workers', default=30, help="Workers count")
@click.option('--reprocessing', is_flag=True)
@click.option('--block-range')
def main(action, log_level, workers, reprocessing, block_range):
    logs.configure(log_level)

    executor.init(workers)

    if reprocessing:
        # disable all stream workers
        block_from, block_until = parse_range(block_range)
        worker = partial(REPROCESSING_MAP[action], block_from, block_until, 100)
        coro = run_reprocessing(worker)
        service_bus.streams = {}
    else:
        # choose only one stream worker which related to action
        service_bus.streams = {k: v for k, v in service_bus.streams.items() if WORKER_MAP[k] == action}
        coro = run_service_bus()

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(coro)
    except KeyboardInterrupt:
        loop.run_until_complete(service_bus.stop())
        loop.stop()
        loop.close()


if __name__ == '__main__':
    main()
