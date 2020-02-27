import asyncio
import logging
from functools import partial
from typing import Any

import mode

from jsearch import settings
from jsearch.common.structs import BlockRange
from jsearch.common.worker import shutdown_root_worker
from jsearch.syncer.database import MainDB, RawDB
from jsearch.syncer.manager import Manager
from jsearch.syncer.state import SyncerState

logger = logging.getLogger(__name__)


class SyncerService(mode.Service):
    def __init__(self,
                 state: SyncerState,
                 sync_range: BlockRange,
                 resync: bool = False,
                 resync_chain_splits: bool = False,
                 **kwargs: Any) -> None:
        self.raw_db = RawDB(settings.JSEARCH_RAW_DB)
        self.main_db = MainDB(settings.JSEARCH_MAIN_DB)
        self.manager = Manager(
            service=self,
            main_db=self.main_db,
            raw_db=self.raw_db,
            sync_range=sync_range,
            resync_chain_splits=resync_chain_splits,
            resync=resync,
            state=state
        )
        super(SyncerService, self).__init__(**kwargs)

    async def on_start(self) -> None:
        await self.raw_db.connect()
        await self.main_db.connect()

    async def on_stop(self) -> None:
        await self.manager.stop()

        await self.main_db.disconnect()
        await self.raw_db.disconnect()

    async def stop(self):
        await self.on_stop()

    async def on_started(self) -> None:
        task = asyncio.create_task(self.syncer())
        task.add_done_callback(partial(shutdown_root_worker, service=self))

    async def syncer(self) -> None:
        await self.manager.start()
        exception = await self.manager.wait()
        if exception:
            await self.crash(exception)
