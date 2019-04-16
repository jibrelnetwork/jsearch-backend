import asyncio
import logging
import signal

from jsearch import settings
from jsearch.syncer.database import MainDB, RawDB
from jsearch.utils import shutdown
from .manager import Manager

logger = logging.getLogger(__name__)


class Service:
    """
    Component container
    """
    _is_need_to_stop: bool = False

    def __init__(self, sync_range):
        self.raw_db = RawDB(settings.JSEARCH_RAW_DB)
        self.main_db = MainDB(settings.JSEARCH_MAIN_DB)
        self.manager = Manager(self, self.main_db, self.raw_db, sync_range=sync_range)

    async def run(self):
        """
        Start all process
        """

        logger.info("Starting jSearch Syncer")

        await self.raw_db.connect()
        await self.main_db.connect()

        asyncio.ensure_future(self.manager.run())
        asyncio.ensure_future(self.monitor())

        logger.info("Up and running!")

    async def stop(self):
        logger.info("Stopping jSearch Syncer")
        await self.manager.stop()
        await self.main_db.disconnect()
        self.raw_db.disconnect()
        logger.info("Bye!")

    def gracefully_shutdown(self):
        self._is_need_to_stop = True

        loop = asyncio.get_event_loop()
        loop.remove_signal_handler(sig=signal.SIGTERM)
        loop.remove_signal_handler(sig=signal.SIGINT)

    async def monitor(self):
        while not self._is_need_to_stop:
            await asyncio.sleep(0.5)

        logger.info('Received exit signal ...')
        logger.info('Stop service bus...')

        await self.stop()

        asyncio.ensure_future(shutdown())

        logger.info('Shutdown complete.')
