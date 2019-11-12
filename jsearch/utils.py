import logging

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from typing import Tuple, Optional

logger = logging.getLogger(__name__)

DEFAULT_RANGE_START = 0
DEFAULT_RANGE_END = None


class Singleton(object):
    _instance = None  # Keep instance reference

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance


def safe_query(db_dsn: str, query: str, key: str) -> Optional[str]:
    engine = create_engine(db_dsn)
    try:
        cursor = engine.execute(query)
        row = cursor.fetchone()
    except ProgrammingError:
        return None

    if row:
        return row[key]

    return None


def get_alembic_version(db_dsn: str) -> Optional[str]:
    query = """
    SELECT version_num FROM alembic_version LIMIT 1;
    """
    return safe_query(db_dsn, query, key='version_num')


def get_goose_version(db_dsn: str) -> Optional[str]:
    query = """
    SELECT version_id, is_applied FROM goose_db_version ORDER BY version_id DESC LIMIT 1;
    """
    return safe_query(db_dsn, query, key='version_id')


def parse_range(value: Optional[str] = None) -> Tuple[int, Optional[int]]:
    """
    >>> parse_range(None)
    (0, None)
    >>> parse_range('')
    (0, None)
    >>> parse_range('10-')
    (10, None)
    >>> parse_range('-10')
    (0, 10)
    >>> parse_range('10-20')
    (10, 20)
    """
    if not value:
        return DEFAULT_RANGE_START, DEFAULT_RANGE_END

    parts = [p.strip() for p in value.split('-')]

    if len(parts) != 2:
        raise ValueError('Invalid sync_range option')

    value_from = int(parts[0]) if parts[0] else DEFAULT_RANGE_START
    value_until = int(parts[1]) if parts[1] else DEFAULT_RANGE_END

    for value in (value_from, value_until):
        if value is not None and value < 0:
            raise ValueError('Invalid range. It allows to be from 0 to integer or - .')

    return value_from, value_until
