import asyncio

import pytest

from jsearch.notable_accounts_worker import services


@pytest.fixture()
@pytest.mark.usefixtures('mock_service_bus', 'db')
async def notable_accounts_service(
        db_connection_string: str,
        loop: asyncio.AbstractEventLoop,
) -> services.NotableAccountsService:

    service = services.NotableAccountsService(
        db_dsn=db_connection_string,
        update_if_exists=True,
        loop=loop
    )

    await service.on_start()
    await service.database.on_start()

    yield service

    await service.database.on_stop()
    await service.on_stop()
