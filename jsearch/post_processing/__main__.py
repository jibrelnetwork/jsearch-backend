# !/usr/bin/env python
import asyncio
import logging
import signal

import click
from mode import Service

from jsearch.common import logs
from jsearch.common.last_block import LastBlock
from jsearch.multiprocessing import executor
from jsearch.post_processing.worker_logs import handle_transaction_logs
from jsearch.post_processing.worker_transfers import handle_new_transfers
from jsearch.service_bus import (
    ROUTE_HANDLE_ERC20_TRANSFERS,
    ROUTE_HANDLE_LAST_BLOCK,
    ROUTE_HANDLE_TRANSACTION_LOGS,
    service_bus,
)
from jsearch.utils import Singleton

logger = logging.getLogger('post_processing')

MODE_STRICT = 'strict'
MODE_FAST = 'fast'

ACTION_PROCESS_LOGS = 'logs'
ACTION_PROCESS_TRANSFERS = 'transfers'

WORKER_MAP = {
    ROUTE_HANDLE_LAST_BLOCK: ACTION_PROCESS_TRANSFERS,
    ROUTE_HANDLE_TRANSACTION_LOGS: ACTION_PROCESS_LOGS,
    ROUTE_HANDLE_ERC20_TRANSFERS: ACTION_PROCESS_TRANSFERS,
}

WORKERS = [handle_new_transfers, handle_transaction_logs]


class PostProcessingWorker(Singleton, Service):
    action: str

    def __init__(self, action: str, *args, **kwargs):
        super(PostProcessingWorker, self).__init__(*args, **kwargs)

        self.action = action
        self._is_need_to_stop = False

    def on_init_dependencies(self):
        service_bus.streams = {k: v for k, v in service_bus.streams.items() if WORKER_MAP[k] == self.action}
        return [service_bus]

    async def on_start(self):
        self._is_need_to_stop = False
        await service_bus.maybe_start()

    async def on_stop(self):
        await service_bus.stop()

    def graceful_stop(self):
        self._is_need_to_stop = True

        loop = asyncio.get_event_loop()
        loop.remove_signal_handler(sig=signal.SIGTERM)
        loop.remove_signal_handler(sig=signal.SIGINT)

    @Service.task
    async def monitor(self):
        while not self._is_need_to_stop:
            await asyncio.sleep(0.5)

        logging.info(f'Received exit signal ...')
        logging.info('Stop service bus...')

        await self.stop()

        asyncio.ensure_future(shutdown())

        logging.info('Shutdown complete.')


async def shutdown():
    loop = asyncio.get_event_loop()

    logging.info(f'Received exit signal ...')
    tasks = [task for task in asyncio.Task.all_tasks() if task is not asyncio.Task.current_task()]

    for task in tasks:
        task.cancel()

    logging.info('Canceling outstanding tasks')

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            logging.info('finished awaiting cancelled tasks, results: {0}'.format(result))

    loop.stop()
    logging.info('Shutdown complete.')


@click.command()
@click.argument('action', type=click.Choice([ACTION_PROCESS_LOGS, ACTION_PROCESS_TRANSFERS]))
@click.option('--log-level', default='INFO', help="Log level")
@click.option('--workers', default=30, help="Workers count")
@click.option('--mode', type=click.Choice([MODE_FAST, MODE_STRICT]), default=MODE_STRICT)
def main(action: str, log_level: str, workers: int, mode: str) -> None:
    logs.configure(log_level)

    service = PostProcessingWorker(action=action)
    executor.init(workers)

    if mode == MODE_FAST:
        last_block = LastBlock()
        last_block.update(number='latest')
        last_block.mode = LastBlock.MODE_READ_ONLY

    coro = service.start()

    loop = asyncio.get_event_loop()
    for signame in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig=signame, callback=service.graceful_stop)

    task = asyncio.ensure_future(coro)
    try:
        loop.run_forever()
    finally:
        loop.close()
        if not task.cancelled():
            task.result()


if __name__ == '__main__':
    main()
