import asyncio
import logging
import time
from typing import Optional

from jsearch import settings
from jsearch.common.processing.erc20_transfer_logs import process_log_operations_bulk
from jsearch.syncer.database import MainDB
from jsearch.utils import add_semaphore, run_on_loop

logger = logging.getLogger(__name__)


@run_on_loop
async def post_processing_operations(
        workers: int = settings.JSEARCH_SYNC_PARALLEL,
        query_limit: Optional[int] = None,
        wait_new_result: bool = False,
        dsn: str = settings.JSEARCH_MAIN_DB
) -> None:
    async with MainDB(connection_string=dsn) as db:
        while True:
            blocks = set()
            started_at = time.time()

            logs = await db.get_logs_to_process_operations(query_limit)
            if not logs:
                if wait_new_result:
                    logger.info("[PROCESSING] There are not logs to read... wait")
                    await asyncio.sleep(5)
                    continue
                else:
                    logger.info("[PROCESSING] There are not logs to read")
                    break

            logs = await process_log_operations_bulk(db, logs, workers)

            update_log = add_semaphore(func=db.update_log, value=workers)
            await asyncio.gather(*[update_log(log) for log in logs])

            working_time = time.time() - started_at

            blocks = {log['block_number'] for log in logs} - blocks
            avg_block_speed = len(blocks) / working_time

            avg_log_speed = len(logs) / working_time

            max_block = max(blocks) if blocks else None
            min_block = min(blocks) if blocks else None

            logger.info("[PROCESSING] speed %0.2f blocks/second", avg_block_speed)
            logger.info("[PROCESSING] speed %0.2f logs/second", avg_log_speed)
            logger.info("[PROCESSING] block range %s - %s", min_block, max_block)
