import pytest
from asynctest import CoroutineMock

from jsearch.common.last_block import LastBlock

pytest_plugins = (
    'jsearch.tests.plugins.last_block',
)


@pytest.fixture()
def last_block():
    last_block = LastBlock()
    yield last_block

    LastBlock._instance = None


@pytest.mark.asyncio
async def test_last_block_loaded_from_kafka(last_block, mock_last_block_consumer):
    # given
    expected_block_number = 6000000
    mock_last_block_consumer({"number": expected_block_number})

    # when
    block_number = await last_block.get()

    # then
    assert block_number == expected_block_number


@pytest.mark.asyncio
async def test_last_block_cache(mocker, last_block):
    # given
    mock = CoroutineMock()

    mocker.patch.object(last_block, 'load', mock)
    last_block.update(number=1)

    # when
    # get last block
    number = await last_block.get()

    # then
    # we await what number was cached
    assert number == 1

    mock.assert_not_called()


def test_last_block_singleton():
    assert id(LastBlock()) == id(LastBlock())
