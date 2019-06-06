import pytest

from jsearch.notable_accounts_worker import services


@pytest.fixture()
@pytest.mark.usefixtures('mock_service_bus', 'db', 'loop')
async def notable_accounts_service(db_connection_string: str) -> services.NotableAccountsService:
    service = services.NotableAccountsService(db_dsn=db_connection_string, update_if_exists=True)

    await service.on_start()
    yield service
    await service.on_stop()
