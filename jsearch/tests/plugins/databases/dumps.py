import json
from pathlib import Path

import pytest

from jsearch.tests.utils import get_dump, sync_blocks


DUMPS_FOLDER = Path(__file__).parent / "dumps"


@pytest.fixture(scope="session")
def db_dump_on_fuck_token_transfer_case(
        db_dsn,
        raw_db_dsn,
        transfer_on_fuck_token_contract
):
    sync_blocks(
        blocks=transfer_on_fuck_token_contract,
        main_db_dsn=db_dsn,
        raw_db_dsn=raw_db_dsn,
    )
    return get_dump(connection_string=db_dsn)


@pytest.fixture(scope="function")
def db_from_fuck_token_transfer_case(request, db, db_dump_on_fuck_token_transfer_case, fill_db, do_truncate_db):
    fill_db(dump=db_dump_on_fuck_token_transfer_case)

    request.addfinalizer(do_truncate_db)


@pytest.fixture(scope='session')
def main_db_dump():
    fixture_path = DUMPS_FOLDER / 'maindb_fixture.json'
    return json.loads(fixture_path.read_text())


@pytest.fixture(scope="function")
def main_db_data(request, db, fill_db, do_truncate_db, main_db_dump):
    fill_db(dump=main_db_dump)

    request.addfinalizer(do_truncate_db)

    return main_db_dump
