import logging
import mode

from jsearch import settings
from jsearch.syncer.database import MainDB, RawDB
from jsearch.syncer.manager import Manager

logger = logging.getLogger(__name__)


class SyncerService(mode.Service):
    def __init__(self, sync_range, *args, **kwargs):
        self.raw_db = RawDB(settings.JSEARCH_RAW_DB)
        self.main_db = MainDB(settings.JSEARCH_MAIN_DB)
        self.manager = Manager(self, self.main_db, self.raw_db, sync_range=sync_range)

        super(SyncerService, self).__init__(*args, **kwargs)

    async def on_start(self) -> None:
        await self.raw_db.connect()
        await self.main_db.connect()
        await self.manager.run()

    async def on_stop(self) -> None:
        await self.manager.stop()
        await self.main_db.disconnect()
        self.raw_db.disconnect()
