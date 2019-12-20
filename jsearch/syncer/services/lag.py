import asyncio
from typing import Any

import functools

import mode

from jsearch import settings
from jsearch.common.async_utils import aiosuppress
from jsearch.common.reference_data import set_lag_statistics
from jsearch.common.worker import shutdown_root_worker
from jsearch.syncer.database import MainDB


class LagCollector(mode.Service):
    main_db: MainDB

    def __init__(self, main_db: MainDB, **kwargs: Any):
        self.main_db = main_db
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        await self.main_db.connect()

    async def on_stop(self) -> None:
        await self.main_db.disconnect()

    async def on_started(self) -> None:
        fut = asyncio.create_task(self.update_lag_statistics())
        fut.add_done_callback(functools.partial(shutdown_root_worker, service=self))

    async def update_lag_statistics(self) -> None:
        while not self.should_stop:
            latest_synced_block_number = await self.main_db.get_latest_synced_block_number()

            with aiosuppress(Exception):
                await set_lag_statistics(latest_synced_block_number)

            await asyncio.sleep(settings.UPDATE_LAG_STATISTICS_DELAY_SECONDS)
