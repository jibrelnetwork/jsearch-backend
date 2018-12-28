import logging
from typing import Optional

from jsearch import settings
from jsearch.post_processing.constants import ACTION_LOG_EVENTS, ACTION_LOG_OPERATIONS
from jsearch.post_processing.worker_events import post_processing_events
from jsearch.post_processing.worker_operations import post_processing_operations

logger = logging.getLogger(__name__)


def post_processing(action: str,
                    workers: int = settings.JSEARCH_SYNC_PARALLEL,
                    query_limit: Optional[int] = None,
                    wait_new_result: bool = False,
                    dsn: str = settings.JSEARCH_MAIN_DB) -> None:
    action_mapping = {
        ACTION_LOG_EVENTS: post_processing_events,
        ACTION_LOG_OPERATIONS: post_processing_operations
    }
    action_func = action_mapping[action]
    action_func(workers=workers, query_limit=query_limit, wait_new_result=wait_new_result, dsn=dsn)
