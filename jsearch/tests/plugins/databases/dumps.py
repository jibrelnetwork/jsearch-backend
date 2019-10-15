import json
import logging

import pytest
from pathlib import Path

DUMPS_FOLDER = Path(__file__).parent / "dumps"

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def main_db_dump():
    fixture_path = DUMPS_FOLDER / 'maindb_fixture.json'
    return json.loads(fixture_path.read_text())


@pytest.fixture(scope='function')
def fill_db(db):
    def _wrapper(dump):

        from jsearch.common.tables import TABLES
        for table in TABLES:
            records = dump.get(table.name)
            if records:
                db.execute(table.insert(), records)
                logger.info('Fill table', extra={'table_name': table.name})
            else:
                logger.info('Table has empty data', extra={'table_name': table.name})

    return _wrapper


@pytest.fixture(scope="function")
def main_db_data(db, fill_db, main_db_dump):
    fill_db(dump=main_db_dump)

    yield main_db_dump
