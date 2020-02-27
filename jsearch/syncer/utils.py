import logging
import time

from sqlalchemy import create_engine

from jsearch import settings

logger = logging.getLogger(__name__)


def execute(connection_string, query: str, *params):
    engine = create_engine(connection_string)

    cursor = engine.execute(query, *params)
    return cursor.fetchone()


def wait_for_version(version_id: str, timeout: float = 5.0) -> None:
    query = "SELECT is_applied FROM goose_db_version WHERE version_id = %s;"

    status = False
    while not status:
        logger.info("Waiting migration", extra={'version_id': version_id, 'timeout': 5})
        row = execute(settings.JSEARCH_MAIN_DB, query, version_id)
        status = row and row['is_applied']
        if not status:
            time.sleep(timeout)


def get_last_block() -> int:
    query = """
    SELECT block_number FROM headers ORDER BY block_number DESC LIMIT 1;
    """
    row = execute(settings.JSEARCH_RAW_DB, query)
    if row:
        return row['block_number']
    return 0
