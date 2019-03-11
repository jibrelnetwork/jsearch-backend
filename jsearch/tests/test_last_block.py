import pytest
from asynctest import CoroutineMock

from jsearch.common.last_block import LastBlock


@pytest.fixture()
def last_block():
    last_block = LastBlock()
    yield last_block

    LastBlock._instance = None


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
