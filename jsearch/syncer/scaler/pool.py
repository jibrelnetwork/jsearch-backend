import logging
import sys
from asyncio import create_subprocess_exec, gather
from dataclasses import dataclass

import signal
from asyncio.subprocess import Process
from typing import Generator, Optional, List

from jsearch import settings
from jsearch.common.structs import SyncRange

logger = logging.getLogger(__name__)


@dataclass
class Worker:
    port: int
    sync_range: SyncRange

    _process: Optional[Process] = None

    async def run(self, resync: bool, log_level: str, no_json_formatter: bool):
        cmd = get_cmd(
            sync_range=self.sync_range,
            port=self.port,
            resync=resync,
            log_level=log_level,
            no_json_formatter=no_json_formatter
        )
        logger.info("Start new worker", extra={"cmd": " ".join(cmd)})
        self._process = await create_subprocess_exec(*cmd, stdout=sys.stdout, stderr=sys.stderr)

    async def stop(self):
        if self._process:
            self._process.send_signal(signal.SIGINT)

    async def wait(self):
        if self._process:
            await self._process.wait()


@dataclass
class WorkersPool:
    sync_range: SyncRange

    workers: int
    _workers: Optional[List[Worker]] = None

    async def run(self, last_block: int, resync: bool, log_level: str, no_json_formatter: bool):
        self._workers = get_workers(self.sync_range, last_block, self.workers)

        tasks = (worker.run(resync, log_level, no_json_formatter) for worker in self._workers)
        await gather(*tasks)

    async def wait(self):
        logger.info('Start workers waiting...')
        if self._workers:
            tasks = (worker.wait() for worker in self._workers)
            await gather(*tasks)

    async def stop(self):
        logger.info('Try to stop workers...')

        if self._workers:
            tasks = (worker.stop() for worker in self._workers)
            await gather(*tasks)
            await self.wait()

        logger.info('Workers have stopped...')


def get_cmd(sync_range: SyncRange, port: int, resync: bool, log_level: str, no_json_formatter: bool):
    cmd = [
        "jsearch-syncer",
        "--port", str(port),
        "--resync" if resync else "",
        "--log-level", log_level,
        "--no_json_formatter" if no_json_formatter else "",
        "--sync-range", str(sync_range)
    ]
    return [item for item in cmd if item]


def scale_range(sync_range: SyncRange, last_block, workers: int = 1) -> Generator[SyncRange, None, None]:
    """
    >>> list(scale_range(SyncRange(0, 100), 100, 2))
    [SyncRange(start=0, end=50), SyncRange(start=50, end=100)]

    >>> list(scale_range(SyncRange(0, None), 90, 2))
    [SyncRange(start=0, end=90), SyncRange(start=90, end=None)]

    >>> list(scale_range(SyncRange(0, None), 90, 3))
    [SyncRange(start=0, end=45), SyncRange(start=45, end=90), SyncRange(start=90, end=None)]
    """
    end = sync_range.end
    if end is None:
        end = last_block
        workers -= 1

    step = int((end - sync_range.start) / workers)
    for start in range(sync_range.start, end, step):
        yield SyncRange(start, end=start + step)

    if sync_range.end is None:
        yield SyncRange(last_block, None)


def get_workers(sync_range: SyncRange, last_block: int, workers: int = 1) -> List[Worker]:
    pool = []
    default_port = settings.SYNCER_API_PORT
    for i, worker_sync_range in enumerate(scale_range(sync_range, last_block, workers), start=1):
        worker = Worker(sync_range=worker_sync_range, port=default_port + i)
        pool.append(worker)

    return pool
