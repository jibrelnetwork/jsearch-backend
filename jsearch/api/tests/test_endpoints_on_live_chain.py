import pytest

from jsearch.tests.entities import TransactionFromDumpWrapper, BlockFromDumpWrapper

pytest_plugins = [
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.databases.dumps',
]


@pytest.mark.live_chain
@pytest.mark.usefixtures('db_from_fuck_token_transfer_case')
async def test_get_block_by_number(cli, db_dump_on_fuck_token_transfer_case):
    # given
    txs = TransactionFromDumpWrapper.from_dump(
        db_dump_on_fuck_token_transfer_case,
        filters={"block_number": 2},
        bulk=True
    )
    block = BlockFromDumpWrapper.from_dump(
        dump=db_dump_on_fuck_token_transfer_case,
        filters={'number': 2},
        transactions=[tx.entity.hash for tx in txs]
    )
    # then
    resp = await cli.get('/v1/blocks/2')
    assert resp.status == 200
    assert await resp.json() == block.as_dict()
