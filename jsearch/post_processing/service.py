import logging
import time
from threading import Thread
from typing import Optional

from jsearch import settings
from jsearch.common.database import MainDBSync
from jsearch.common.processing.logs import process_logs
from jsearch.common.processing.operations import do_operations_bulk

logger = logging.getLogger(__name__)


def post_processing(sleep_interval: int = 10) -> None:
    contract_cache = {}
    with MainDBSync(connection_string=settings.JSEARCH_MAIN_DB) as db:
        while True:
            with db.conn.begin():
                start_time = time.monotonic()
                logs = db.get_logs_for_post_processing(conn=db.conn)
                if logs:
                    logs, operations = process_logs(logs, contracts_cache=contract_cache)
                    do_operations_bulk(db, operations=operations)

                    db.update_logs(db.conn, logs)
                    logger.info(
                        "%s logs processed on %0.2f s",
                        len(logs), start_time - time.monotonic()
                    )
                else:
                    logger.info("There are not logs to post processing... sleep to %s seconds", sleep_interval)
                    time.sleep(sleep_interval)

            if not service.is_running:
                break


class PostProcessingService:
    is_running: bool = False
    worker: Optional[Thread] = None

    def run(self) -> None:
        self.is_running = True
        self.worker = Thread(target=post_processing)
        self.worker.run()

    def stop(self) -> None:
        self.is_running = False


service = PostProcessingService()
