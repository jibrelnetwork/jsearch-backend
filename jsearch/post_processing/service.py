import logging
import time
from threading import Thread
from typing import Optional

from jsearch import settings
from jsearch.common.database import MainDBSync
from jsearch.common.processing.logs import process_logs
from jsearch.common.processing.tokens import update_token_info_bulk, update_token_balance_bulk

logger = logging.getLogger(__name__)


def post_processing(sleep_interval: int = 10) -> None:
    with MainDBSync(connection_string=settings.JSEARCH_MAIN_DB) as db:
        while True:
            with db.conn.begin():
                logs = db.get_logs_for_post_processing(conn=db.conn)
                if logs:
                    logger.info('New logs to post processing...')
                    logs, need_update_token_info, need_update_token_balance = process_logs(logs)

                    update_token_info_bulk(need_update_token_info)
                    update_token_balance_bulk(need_update_token_balance)

                    db.update_logs(db.conn, logs)
                    logger.info('Processed %s logs records', len(logs))
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
