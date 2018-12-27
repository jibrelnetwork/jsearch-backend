import logging
import time
from concurrent.futures import as_completed
from concurrent.futures.process import ProcessPoolExecutor
from functools import partial
from typing import List, Optional

from jsearch import settings
from jsearch.common.database import MainDBSync
from jsearch.common.processing.logs import process_log_event
from jsearch.typing import Log

logger = logging.getLogger(__name__)


def post_processing_events_worker(dsn, logs: List[Log]):
    with MainDBSync(connection_string=dsn) as db:
        for log in logs:
            log = process_log_event(log)
            db.update_log(record=log)


def post_processing_events(
        workers: int = settings.JSEARCH_SYNC_PARALLEL,
        query_limit: Optional[int] = None,
        wait_new_result: bool = False,
        dsn: str = settings.JSEARCH_MAIN_DB
) -> None:
    with MainDBSync(connection_string=dsn) as db:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            while True:
                blocks = set()
                started_at = time.time()
                logs = db.get_logs_to_process_events(query_limit)
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

                    func = partial(post_processing_events_worker, dsn, chunk)
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
