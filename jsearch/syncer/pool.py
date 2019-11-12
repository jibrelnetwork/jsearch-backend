import asyncio
import logging
import sys
from asyncio import create_subprocess_exec, gather, Lock
from dataclasses import dataclass

import signal
import time
from aiohttp import ClientSession, ClientError
from asyncio.subprocess import Process
from typing import Generator, Optional, List, Dict, Any

from jsearch import settings
from jsearch.common.structs import BlockRange

logger = logging.getLogger(__name__)


@dataclass
class Worker:
    port: int
    kwargs: Dict[str, Any]
    sync_range: BlockRange

    _cmd: Optional[List[str]] = None
    _process: Optional[Process] = None

    async def run(self):
        cmd = get_cmd(
            sync_range=self.sync_range,
            port=self.port,
            **self.kwargs
        )
        logger.info("Start new worker", extra={"cmd": " ".join(cmd)})
        self._cmd = cmd
        self._process = await create_subprocess_exec(*cmd, stdout=sys.stdout, stderr=sys.stderr)

    def kill(self):
        try:
            self._process.terminate()
        except Exception as e:
            logging.info(e)

    async def stop(self):
        if self.is_working:
            self._process.send_signal(signal.SIGINT)

    async def wait(self):
        if self.is_working:
            await self._process.wait()
            if self._process.returncode != 0:
                raise RuntimeError(f'Worker ({" ".join(self._cmd)}) '
                                   f'has finished with exit code {self._process.returncode}')

    async def check_healthy(self):
        if self.is_working:
            return await request_healthcheck(port=self.port)
        return {'healthy': True}

    async def describe(self):
        state = {}
        if self._process:
            try:
                state = await request_state(self.port)
            except ClientError as e:
                logging.warning(e)
                state = {}

        return {
            'port': self.port,
            'range': self.sync_range,
            'is_working': self.is_working,
            'range_synced': not self.is_working,
            **state,
        }

    @property
    def is_working(self):
        return self._process is not None and self._process.returncode is None


WAIT_PROCESS_TIMEOUT: int = 60


@dataclass
class WorkersPool:
    sync_range: BlockRange

    workers: int
    worker_kwargs: Dict[str, Any]

    _lock: Lock = Lock()
    _workers: Optional[List[Worker]] = None
    _rescaling_in_progress: bool = False

    _is_need_to_stop: bool = False

    async def run(self, last_block: int):
        self._workers = get_workers(self.sync_range, last_block, self.workers, **self.worker_kwargs)

        tasks = (worker.run() for worker in self._workers)
        await gather(*tasks)

    async def terminate(self):
        self._is_need_to_stop = True

        await self.stop()
        await self.wait()

    async def stop(self):
        logger.info('Try to stop workers...')

        if self._workers:
            tasks = (worker.stop() for worker in self._workers)
            await gather(*tasks)

    async def wait(self):
        while not self._is_need_to_stop:
            await asyncio.sleep(1)

    async def wait_until_workers_have_stopped(self):
        start_time = time.monotonic()

        while any(worker.is_working for worker in self._workers):
            logger.info('Wait until workers have stopped...)')
            await asyncio.sleep(5)

            if time.monotonic() - start_time > WAIT_PROCESS_TIMEOUT:
                logger.info('Try to kill all workers...)')
                for worker in self._workers:
                    worker.kill()

                await asyncio.sleep(5)

    async def scale(self, sync_range: BlockRange, workers: int, last_block: int):
        logging.info('[SCALE]: start')
        await self.stop()
        await self.wait_until_workers_have_stopped()

        self.workers = workers
        self.sync_range = sync_range

        await self.run(last_block=last_block)

    async def check_healthy(self) -> List[Dict[str, Any]]:
        # FIXME (nickgashkov): `self._workers` could be `None`.
        tasks = (worker.check_healthy() for worker in self._workers)  # type: ignore
        return await gather(*tasks)

    async def describe(self):
        states = await gather(*[worker.describe() for worker in self._workers])
        blocks = sum([state.get("blocks", 0) for state in states], 0)
        pool_speed = sum([state.get('speed') for state in states if state.get('speed')], 0)
        return {
            'sync_range': str(self.sync_range),
            'workers': {
                'count': self.workers,
                'ranges': states,
                'speed': round(pool_speed, 3),
                'blocks': blocks,
            },
        }


async def request_healthcheck(port: int) -> Dict[str, Any]:
    async with ClientSession() as session:
        async with session.get(f'http://localhost:{port}/healthcheck') as response:
            if response.status == 200:
                return await response.json()
            return {'healthy': False}


async def request_state(port: int) -> Dict[str, Any]:
    async with ClientSession() as session:
        async with session.get(f'http://localhost:{port}/state') as response:
            if response.status == 200:
                return await response.json()
            return {}


def get_cmd(sync_range: BlockRange, port: int, **kwargs: Any):
    cmd = [
        "jsearch-syncer",
        "--port", str(port),
        "--sync-range", str(sync_range)
    ]
    for key, value in kwargs.items():
        arg_key = f"--{key.replace('_', '-')}"

        if isinstance(value, bool):
            if value:
                cmd.append(arg_key)
        elif isinstance(value, int):
            cmd.append(arg_key)
            cmd.append(str(value))

    logging.info("Worker", extra={"cmd": " ".join(cmd)})
    return cmd


def scale_range(sync_range: BlockRange, last_block, workers: int = 1) -> Generator[BlockRange, None, None]:
    """Scales provided `sync_range` for multiple `workers` to process.

    Scaling is done in a following manner (5 workers for example from 0 to
    latest block 100):

        start        - start+step*1-1  |  0     - 25*1-1  | 0   - 24
        start+step*1 - start+step*2-1  |  25*1  - 25*2-1  | 25  - 49
        start+step*2 - start+step*3-1  |  25*2  - 25*3-1  | 50  - 74
        start+step*3 - start+step*4-1  |  25*3  - 25*4-1  | 75  - 99
        start+step*4 - end             |  25*4  - None    | 100 - None


    Examples:
        >>> list(scale_range(BlockRange(0, None), 200, 1))
        [BlockRange(start=0, end=None)]

        >>> list(scale_range(BlockRange(0, 100), 100, 2))
        [BlockRange(start=0, end=49), BlockRange(start=50, end=100)]

        >>> list(scale_range(BlockRange(0, None), 90, 2))
        [BlockRange(start=0, end=89), BlockRange(start=90, end=None)]

        >>> list(scale_range(BlockRange(0, None), 90, 3))
        [BlockRange(start=0, end=44), BlockRange(start=45, end=89), BlockRange(start=90, end=None)]

        >>> list(scale_range(BlockRange(0, 50), 8740094, 3))  # Uneven ranges.
        [BlockRange(start=0, end=15), BlockRange(start=16, end=31), BlockRange(start=32, end=50)]

        >>> list(scale_range(BlockRange(3500000, None), 8740094, 5)) == [
        ...     BlockRange(start=3500000, end=4810022),
        ...     BlockRange(start=4810023, end=6120045),
        ...     BlockRange(start=6120046, end=7430068),
        ...     BlockRange(start=7430069, end=8740091),
        ...     BlockRange(start=8740092, end=None),
        ... ]
        True
    """
    step = _get_sync_range_step(sync_range, last_block, workers)

    for x in range(workers - 1):
        yield BlockRange(
            start=sync_range.start + step * x,
            end=sync_range.start + step * (x + 1) - 1,
        )

    yield BlockRange(
        start=sync_range.start + step * (workers - 1),
        end=sync_range.end,
    )


def get_workers(sync_range: BlockRange, last_block: int, workers: int = 1, **kwargs: Dict[str, Any]) -> List[Worker]:
    pool = []
    default_port = settings.SYNCER_API_PORT
    for i, worker_sync_range in enumerate(scale_range(sync_range, last_block, workers), start=1):
        worker = Worker(
            sync_range=worker_sync_range,
            port=default_port + i,
            kwargs=kwargs
        )
        pool.append(worker)

    return pool


def _get_sync_range_step(sync_range: BlockRange, last_block, workers: int) -> int:
    start = sync_range.start
    end = last_block if sync_range.end is None else sync_range.end

    if sync_range.end is None:
        # WTF: This increases ranges' chunks and allows running last worker
        # closer to the network's end:
        #     0-None, 50, 5 -> 0-11, 12-23, 24-35, 36-47, 48-None
        #
        # Without workers decrease, ranges will be like that with the last one
        # further from the last block (50):
        #     0-None, 50, 5 -> 0-9, 10-19, 20-29, 30-39, 40-None

        workers = workers - 1

    return (end - start) // max(workers, 1)
