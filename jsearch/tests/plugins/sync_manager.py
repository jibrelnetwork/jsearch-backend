import pytest

from jsearch.syncer import database, manager


@pytest.mark.asyncio
@pytest.fixture()
async def syncer_manager(loop, db_connection_string, raw_db_connection_string):
    main_db_wrapper = database.MainDB(db_connection_string)
    raw_db_wrapper = database.RawDB(raw_db_connection_string)

    syncer_manager = manager.Manager(main_db_wrapper, raw_db_wrapper, (0, None))

    await raw_db_wrapper.connect()
    await main_db_wrapper.connect()

    yield syncer_manager

    await main_db_wrapper.disconnect()
    raw_db_wrapper.disconnect()
