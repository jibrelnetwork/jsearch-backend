# !/usr/bin/env python
import logging
import os

import click

from jsearch.common import logs, worker
from jsearch.common.last_block import LastBlock
from jsearch.multiprocessing import executor
from jsearch.post_processing import services

logger = logging.getLogger('post_processing')

MODE_STRICT = 'strict'
MODE_FAST = 'fast'


@click.command()
@click.argument('action', type=click.Choice(services.ACTION_PROCESS_CHOICES))
@click.option('--log-level', default=os.getenv('LOG_LEVEL', 'INFO'), help="Log level")
@click.option('--workers', default=30, help="Workers count")
@click.option('--mode', type=click.Choice([MODE_FAST, MODE_STRICT]), default=MODE_STRICT)
def main(action: str, log_level: str, workers: int, mode: str) -> None:
    logs.configure(log_level)
    executor.init(workers)

    if mode == MODE_FAST:
        last_block = LastBlock()
        last_block.update(number='latest')
        last_block.mode = LastBlock.MODE_READ_ONLY

    worker.Worker(
        services.PostProcessingService(action=action),
        services.ApiService(),
    ).execute_from_commandline()


if __name__ == '__main__':
    main()
