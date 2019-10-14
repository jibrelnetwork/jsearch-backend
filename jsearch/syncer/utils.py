from sqlalchemy import create_engine

from jsearch import settings


def get_last_block() -> int:
    query = """
    SELECT block_number FROM headers ORDER BY block_number DESC LIMIT 1;
    """

    engine = create_engine(settings.JSEARCH_RAW_DB)
    cursor = engine.execute(query)
    row = cursor.fetchone()

    if row:
        return row['block_number']
    return 0
