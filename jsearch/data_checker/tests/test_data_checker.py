from decimal import Decimal

from aiohttp import web
import pytest

from jsearch.data_checker.service import DataChecker, Transfer
from jsearch.tests.plugins.databases.factories.blocks import BlockFactory
from jsearch.tests.plugins.databases.factories.common import generate_hash
from jsearch.tests.plugins.databases.factories.token_transfers import TokenTransferFactory

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def data_checker(db_dsn: str) -> DataChecker:
    service = DataChecker(main_db_dsn=db_dsn, use_proxy=False)

    await service.on_start()
    yield service
    await service.on_stop()


async def test_get_last_synced_block_retrieves_the_last_block_with_an_offset_of_6_blocks(
        data_checker: DataChecker,
        block_factory: BlockFactory,
) -> None:
    block_factory.reset_sequence(1)
    blocks = block_factory.create_batch(7)

    block_from_checker = await data_checker.get_last_synced_block()

    assert block_from_checker['hash'] == blocks[-6].hash


async def test_get_synced_block_by_number_retrieves_the_block(
        data_checker: DataChecker,
        block_factory: BlockFactory,
) -> None:
    block = block_factory.create()
    block_from_checker = await data_checker.get_synced_block_by_number(block.number)

    assert block_from_checker['hash'] == block.hash


async def test_get_synced_block_transfers_retrieves_transfers(
        data_checker: DataChecker,
        transfer_factory: TokenTransferFactory,
) -> None:
    block_hash = generate_hash()

    transfers = transfer_factory.create_batch(2, block_hash=block_hash)
    transfers = [
        Transfer(
            from_address=transfer.from_address,
            to_address=transfer.to_address,
            token_address=transfer.token_address,
            amount=Decimal(transfer.token_value) / 10 ** transfer.token_decimals,
            transaction_hash=transfer.transaction_hash,
        )
        for transfer in transfers
    ]

    transfers_from_checker = await data_checker.get_synced_block_transfers(block_hash)

    assert set(transfers_from_checker) == set(transfers)


async def test_proxies_can_be_loaded(aiohttp_raw_server, db_dsn) -> None:
    async def handler(request):
        return web.Response(
            text=(
                "209.205.212.34:1200" + "\n"
                "209.205.212.34:1201" + "\n"
                "209.205.212.34:1202" + "\n"
                "209.205.212.34:1203" + "\n"
                "209.205.212.34:1204" + "\n" + "\n"
            ),
        )

    proxy_server = await aiohttp_raw_server(handler)
    proxy_server_load_url = proxy_server.make_url("/rotating/megaproxy/")
    data_checker = DataChecker(main_db_dsn=db_dsn, use_proxy=True, proxy_load_url=str(proxy_server_load_url))

    await data_checker.load_proxies()

    assert data_checker.proxy_list == [
        "209.205.212.34:1200",
        "209.205.212.34:1201",
        "209.205.212.34:1202",
        "209.205.212.34:1203",
        "209.205.212.34:1204",
    ]
