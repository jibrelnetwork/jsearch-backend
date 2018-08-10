import asyncio
import json
import logging
import time

from jsearch.common.database import DatabaseError


logger = logging.getLogger(__name__)


SLEEP_ON_ERROR_DEFAULT = 0.1
SLEEP_ON_DB_ERROR_DEFAULT = 5
SLEEP_ON_NO_BLOCKS_DEFAULT = 1


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
        self.sleep_on_no_blocks = SLEEP_ON_NO_BLOCKS_DEFAULT

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
                start_time = time.monotonic()
                synced_blocks_cnt = 0
                blocks_to_sync = await self.get_blocks_to_sync()
                if len(blocks_to_sync) == 0:
                    await asyncio.sleep(self.sleep_on_no_blocks)
                    continue

                for block in blocks_to_sync:
                   is_sync_ok = await self.sync_block(block["block_number"])
                   if is_sync_ok is False:
                       break  # FIXME!
                       logger.debug("Block #%s sync failed", block["block_number"])
                   else:
                       synced_blocks_cnt += 1

                # results = await asyncio.gather(*[self.sync_block(b["block_number"]) for b in blocks_to_sync])
                # synced_blocks_cnt = sum(results)
                sync_time = time.monotonic() - start_time
                avg_time = sync_time / synced_blocks_cnt if synced_blocks_cnt else 0
                logger.info("%s blocks synced on %ss, avg time %ss", synced_blocks_cnt, sync_time, avg_time)
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
        if latest_block_num is None:
            start_block_num = 0
        else:
            start_block_num = latest_block_num + 1
        blocks = await self.raw_db.get_blocks_to_sync(start_block_num, self.chunk_size)
        logger.info("Latest synced block num is %s, %s blocks to sync", latest_block_num, len(blocks))
        return blocks

    async def sync_block(self, block_number):
        start_time = time.monotonic()
        is_block_exist = await self.main_db.is_block_exist(block_number)
        if is_block_exist is True:
            logger.debug("Block #%s exist", block_number)
            return False
        receipts = await self.raw_db.get_block_receipts(block_number)
        if receipts is None:
            logger.debug("Block #%s not ready: no receipts", block_number)
            return False

        results = await asyncio.gather(
            self.raw_db.get_header_by_hash(block_number),
            self.raw_db.get_block_accounts(block_number),
            self.raw_db.get_block_body(block_number),
            self.raw_db.get_reward(block_number),
            self.raw_db.get_internal_transactions(block_number),
        )

        header = results[0]
        accounts = results[1]
        body = results[2]
        reward = results[3]
        internal_transactions = results[4]

        body_fields = json.loads(body['fields'])
        uncles = body_fields['Uncles'] or []
        transactions = body_fields['Transactions'] or []

        await self.main_db.write_block(header=header, uncles=uncles, accounts=accounts,
                                       transactions=transactions, receipts=receipts, reward=reward,
                                       internal_transactions=internal_transactions)
        sync_time = time.monotonic() - start_time
        logger.debug("Block #%s synced on %ss", block_number, sync_time)
        return True
