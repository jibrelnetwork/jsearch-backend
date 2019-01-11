import logging
import time
from concurrent.futures import as_completed
from concurrent.futures.process import ProcessPoolExecutor
from functools import partial
from typing import Callable, List, Optional

from jsearch import settings
from jsearch.common.database import MainDBSync
from jsearch.common.processing.erc20_transfer_logs import process_log_operations_bulk
from jsearch.common.processing.logs import process_log_event
from jsearch.typing import Log
from jsearch.utils import suppress_exception

ACTION_LOG_EVENTS = 'events'
ACTION_LOG_OPERATIONS = 'operations'

logger = logging.getLogger(__name__)

Worker = Callable[[List[Log], Optional[str]], None]
Query = Callable[[int], List[Log]]


@suppress_exception
def log_event_processing_worker(logs: List[Log], dsn: str = settings.JSEARCH_MAIN_DB):
    with MainDBSync(connection_string=dsn) as db:
        for log in logs:
            log = process_log_event(log)
            db.update_log(record=log)


@suppress_exception
def log_operations_processing_worker(logs: List[Log], dsn: str = settings.JSEARCH_MAIN_DB):
    with MainDBSync(connection_string=dsn) as db:
        logs = process_log_operations_bulk(db, logs)
        for log in logs:
            db.update_log(record=log)


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


def post_processing(action: str,
                    workers: int = settings.JSEARCH_SYNC_PARALLEL,
                    query_limit: Optional[int] = None,
                    wait_new_result: bool = False,
                    dsn: str = settings.JSEARCH_MAIN_DB) -> None:
    with MainDBSync(connection_string=dsn) as db:
        worker = get_worker(action)
        query = get_query(action, db)

        with ProcessPoolExecutor(max_workers=workers) as executor:
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

                tasks = []
                chunk_size = int(len(logs) / (workers - 1)) or 1
                for offset in range(0, len(logs), chunk_size):
                    chunk = logs[offset: offset + chunk_size]

                    func = partial(worker, chunk)
                    task = executor.submit(func)
                    tasks.append(task)

                if tasks:
                    for future in as_completed(tasks):
                        future.result()

                working_time = time.time() - started_at

                blocks = {log['block_number'] for log in logs} - blocks
                avg_block_speed = len(blocks) / working_time

                avg_log_speed = len(logs) / working_time

                max_block = max(blocks) if blocks else None
                min_block = min(blocks) if blocks else None

                logger.info("[PROCESSING] speed %0.2f blocks/second", avg_block_speed)
                logger.info("[PROCESSING] speed %0.2f logs/second", avg_log_speed)
                logger.info("[PROCESSING] block range %s - %s", min_block, max_block)
