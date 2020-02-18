import asyncio
import functools
from typing import Any

import mode

from jsearch.common.reference_data import set_lag_statistics
from jsearch.common.worker import shutdown_root_worker
from jsearch.monitor.db import MainDB


class LagCollector(mode.Service):
    main_db: MainDB

    def __init__(self, main_db: MainDB, **kwargs: Any):
        self.main_db = main_db
        super().__init__(**kwargs)

    async def on_started(self) -> None:
        task = asyncio.create_task(self.update_lag_statistics())
        task.add_done_callback(functools.partial(shutdown_root_worker, service=self))

    async def update_lag_statistics(self) -> None:
        while not self.should_stop:
            latest_synced_block_number = await self.main_db.get_latest_synced_block_number()

            await set_lag_statistics(latest_synced_block_number)
            await asyncio.sleep(1)
