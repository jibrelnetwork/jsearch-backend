# !/usr/bin/env python
import asyncio
import logging

import click
import signal
from mode import Service

from jsearch.common import logs
from jsearch.post_processing.database import db_service
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
from jsearch.utils import Singleton, shutdown, add_gracefully_shutdown_handlers

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
        return [
            service_bus,
            db_service,
        ]

    async def on_start(self):
        self._is_need_to_stop = False
        await service_bus.maybe_start()

    async def on_stop(self):
        await service_bus.stop()

    def gracefully_shutdown(self):
        self._is_need_to_stop = True

        loop = asyncio.get_event_loop()
        loop.remove_signal_handler(sig=signal.SIGTERM)
        loop.remove_signal_handler(sig=signal.SIGINT)

    @Service.task
    async def monitor(self):
        while not self._is_need_to_stop:
            await asyncio.sleep(0.5)

        logger.info('Received exit signal ...')
        logger.info('Stop service bus...')

        await self.stop()

        asyncio.ensure_future(shutdown())

        logger.info('Shutdown complete.')


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
    task = asyncio.ensure_future(coro)

    add_gracefully_shutdown_handlers(service.gracefully_shutdown)
    try:
        loop.run_forever()
    finally:
        loop.close()
        if not task.cancelled():
            task.result()


if __name__ == '__main__':
    main()
