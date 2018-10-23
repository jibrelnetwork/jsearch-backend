import asyncio
import logging

from jsearch.common.database import MainDB, RawDB
from jsearch import settings
from .manager import Manager


logger = logging.getLogger(__name__)


class Service:
    """
    Component container
    """
    def __init__(self, options):
        self.options = options
        self.raw_db = RawDB(settings.JSEARCH_RAW_DB)
        self.main_db = MainDB(settings.JSEARCH_MAIN_DB)
        self.manager = Manager(self, self.main_db, self.raw_db)

    def run(self):
        """
        Start all process
        """
        logger.info("Starting jSearch Syncer")

        loop = asyncio.get_event_loop()

        loop.run_until_complete(self.raw_db.connect())
        loop.run_until_complete(self.main_db.connect())

        asyncio.ensure_future(self.manager.run())

        logger.info("Up and running!")

    def stop(self):
        logger.info("Stopping jSearch Syncer")
        self.manager.stop()
        self.main_db.disconnect()
        self.raw_db.disconnect()
        logger.info("Bye!")
