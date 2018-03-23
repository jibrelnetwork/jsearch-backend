import asyncio
import json
import logging
import time

from .database import DatabaseError


logger = logging.getLogger(__name__)


SLEEP_ON_ERROR_DEFAULT = 0.1
SLEEP_ON_DB_ERROR_DEFAULT = 5


class Manager:
    """
    Sync manager
    """
    def __init__(self, service, main_db, raw_db):
        self.service = service
        self.main_db = main_db
        self.raw_db = raw_db
        self._running = False
        self.chunk_size = 10
        self.sleep_on_db_error = SLEEP_ON_DB_ERROR_DEFAULT
        self.sleep_on_error = SLEEP_ON_ERROR_DEFAULT

    async def run(self):
        logger.info("Starting Sync Manager")
        self._running = True
        asyncio.ensure_future(self.sequence_sync_loop())

    def stop(self):
        self._running = False

    async def sequence_sync_loop(self):
        logger.info("Entering Sequence Sync Loop")
        while self._running is True:
            try:
                blocks_to_sync = await self.get_blocks_to_sync()
                for block in blocks_to_sync:
                    await self.sync_block(block["block_number"])
            except DatabaseError:
                logger.exception("Database Error accured:")
                await asyncio.sleep(self.sleep_on_db_error)
            except Exception:
                logger.exception("Error accured:")
                await asyncio.sleep(self.sleep_on_error)
                self.sleep_on_error = self.sleep_on_error * 2
            else:
                self.sleep_on_error = SLEEP_ON_ERROR_DEFAULT

    async def get_blocks_to_sync(self):
        latest_block_num = await self.main_db.get_latest_sequence_synced_block_number()
        logger.info("Latest synced block num is %s", latest_block_num)
        if latest_block_num is None:
            start_block_num = 0
        else:
            start_block_num = latest_block_num + 1
        blocks = await self.raw_db.get_blocks_to_sync(start_block_num, self.chunk_size)
        return blocks

    async def sync_block(self, block_hash):
        start_time = time.monotonic()
        header = await self.raw_db.get_header_by_hash(block_hash)
        accounts = await self.raw_db.get_block_accounts(block_hash)
        body = await self.raw_db.get_block_body(block_hash)
        body_fields = json.loads(body['fields'])
        uncles = body_fields['Uncles'] or []
        transactions = body_fields['Transactions'] or []
        receipts = await self.raw_db.get_block_receipts(block_hash)

        await self.main_db.write_block(header=header, uncles=uncles, accounts=accounts,
                                       transactions=transactions, receipts=receipts)
        sync_time = time.monotonic() - start_time
        logger.info("Block #%s synced on %ss", header['block_number'], sync_time)
