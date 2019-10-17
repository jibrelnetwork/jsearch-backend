import json

import pytest
from pathlib import Path

DUMPS_FOLDER = Path(__file__).parent / "dumps"


@pytest.fixture(scope='session')
def main_db_dump():
    fixture_path = DUMPS_FOLDER / 'maindb_fixture.json'
    return json.loads(fixture_path.read_text())


@pytest.fixture(scope="function")
def main_db_data(request, db, fill_db, do_truncate_db, main_db_dump):
    fill_db(dump=main_db_dump)

    request.addfinalizer(do_truncate_db)

    return main_db_dump
