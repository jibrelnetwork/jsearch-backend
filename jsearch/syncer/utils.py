from sqlalchemy import create_engine

from jsearch import settings


def get_last_block() -> int:
    query = """
    SELECT number FROM blocks ORDER BY number DESC LIMIT 1;
    """

    engine = create_engine(settings.JSEARCH_MAIN_DB)
    cursor = engine.execute(query)
    row = cursor.fetchone()

    if row:
        return row['number']
    return 0
