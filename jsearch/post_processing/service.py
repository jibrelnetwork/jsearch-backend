import logging
import time
from typing import Callable, List, Optional

from jsearch import settings
from jsearch.common.database import MainDBSync
from jsearch.common.processing.erc20_transfer_logs import (
    fetch_contracts,
    logs_to_transfers,
    process_log_operations_bulk,
    fetch_blocks
)
from jsearch.common.processing.logs import process_log_event
from jsearch.kafka.producer import get_producer
from jsearch.typing import Log

ACTION_LOG_EVENTS = 'events'
ACTION_LOG_OPERATIONS = 'operations'

logger = logging.getLogger(__name__)

Worker = Callable[[List[Log], Optional[str]], None]
Query = Callable[[int], List[Log]]


async def log_event_processing_worker(logs: List[Log], dsn: str = settings.JSEARCH_MAIN_DB):
    with MainDBSync(connection_string=dsn) as db:
        for log in logs:
            log = process_log_event(log)
            db.update_log(record=log)


async def log_operations_processing_worker(logs: List[Log], dsn: str = settings.JSEARCH_MAIN_DB):
    producer = get_producer()
    with MainDBSync(connection_string=dsn) as db:
        try:
            await producer.start()
            contracts = await fetch_contracts(producer, logs)
            logs = process_log_operations_bulk(db, logs, contracts)

            blocks = fetch_blocks(db, logs)
            transfers = logs_to_transfers(logs, blocks, contracts)

            db.insert_transfers(transfers)
            for log in logs:
                log['is_transfer_processed'] = True
                db.update_log(record=log)
        finally:
            await producer.stop()


def get_worker(action: str) -> Worker:
    worker_map = {
        ACTION_LOG_EVENTS: log_event_processing_worker,
        ACTION_LOG_OPERATIONS: log_operations_processing_worker
    }
    return worker_map[action]


def get_query(action: str, db: MainDBSync) -> Query:
    query_map = {
        ACTION_LOG_EVENTS: db.get_logs_to_process_events,
        ACTION_LOG_OPERATIONS: db.get_logs_to_process_operations
    }
    return query_map[action]


async def post_processing(action: str,
                          workers: int = settings.JSEARCH_SYNC_PARALLEL,
                          query_limit: Optional[int] = None,
                          wait_new_result: bool = False,
                          dsn: str = settings.JSEARCH_MAIN_DB) -> None:
    with MainDBSync(connection_string=dsn) as db:
        worker = get_worker(action)
        query = get_query(action, db)

        while True:
            blocks = set()
            started_at = time.time()
            logs = query(query_limit)
            if not logs:
                if wait_new_result:
                    logger.info("[PROCESSING] There are not logs to read... wait")
                    time.sleep(5)
                    continue
                else:
                    logger.info("[PROCESSING] There are not logs to read")
                    break

            # tasks = []
            chunk_size = int(len(logs) / (workers - 1)) or 1
            for offset in range(0, len(logs), chunk_size):
                chunk = logs[offset: offset + chunk_size]
                await worker(chunk)

                # tasks.append(task)

            # if tasks:
            #     await asyncio.gather(*tasks)

            working_time = time.time() - started_at

            blocks = {log['block_number'] for log in logs} - blocks
            avg_block_speed = len(blocks) / working_time

            avg_log_speed = len(logs) / working_time

            max_block = max(blocks) if blocks else None
            min_block = min(blocks) if blocks else None

            logger.info("[PROCESSING] speed %0.2f blocks/second", avg_block_speed)
            logger.info("[PROCESSING] speed %0.2f logs/second", avg_log_speed)
            logger.info("[PROCESSING] block range %s - %s", min_block, max_block)
