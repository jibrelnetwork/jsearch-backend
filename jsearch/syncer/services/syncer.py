import logging

import mode
from typing import Any, Optional, Tuple

from jsearch import settings
from jsearch.syncer.database import MainDB, RawDB
from jsearch.syncer.manager import Manager
from jsearch.syncer.state import SyncerState

logger = logging.getLogger(__name__)


class SyncerService(mode.Service):
    def __init__(self,
                 state: SyncerState,
                 sync_range: Tuple[int, int],
                 resync: Optional[bool] = False,
                 *args: Any,
                 **kwargs: Any) -> None:
        self.raw_db = RawDB(settings.JSEARCH_RAW_DB)
        self.main_db = MainDB(settings.JSEARCH_MAIN_DB)
        self.manager = Manager(self, self.main_db, self.raw_db, sync_range=sync_range, resync=resync, state=state)

        super(SyncerService, self).__init__(*args, **kwargs)

    async def on_start(self) -> None:
        await self.raw_db.connect()
        await self.main_db.connect()

    async def on_stop(self) -> None:
        await self.manager.stop()

        await self.main_db.disconnect()
        await self.raw_db.disconnect()

    @mode.Service.task
    async def syncer(self):
        await self.manager.start()
        exception = await self.manager.wait()
        if exception:
            await self.crash(exception)

        await self.stop()
        # we schedule shutdown on root Worker
        self.beacon.root.data.schedule_shutdown()
