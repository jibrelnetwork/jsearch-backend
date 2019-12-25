import asyncio

import functools
import mode
from aiohttp import web
from typing import Any

from jsearch import settings
from jsearch.api.handlers import monitoring
from jsearch.api.middlewares import cors_middleware
from jsearch.common.async_utils import aiosuppress
from jsearch.common.reference_data import set_lag_statistics
from jsearch.common.worker import shutdown_root_worker
from jsearch.syncer.database import MainDB


def make_app() -> web.Application:
    application = web.Application(middlewares=[cors_middleware])
    application.router.add_route('GET', '/metrics', monitoring.metrics)

    return application


class LagCollector(mode.Service):
    main_db: MainDB

    def __init__(self, **kwargs: Any):
        self.main_db = MainDB(settings.JSEARCH_MAIN_DB)
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        await self.main_db.connect()

    async def on_stop(self) -> None:
        await self.main_db.disconnect()

    async def on_started(self) -> None:
        task = asyncio.create_task(self.update_lag_statistics())
        task.add_done_callback(functools.partial(shutdown_root_worker, service=self))

    async def update_lag_statistics(self) -> None:
        while not self.should_stop:
            latest_synced_block_number = await self.main_db.get_latest_synced_block_number()

            with aiosuppress(Exception):
                await set_lag_statistics(latest_synced_block_number)

            await asyncio.sleep(1)
