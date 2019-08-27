"""
We have a fixture with split chain events:
  - from 8020425
  - until 8020428

We have a chain:
    - 8020425: 0xc612bbb64b83b23729ff542d7ced2617d028834681893a8ea28bb2dcfc01ddc9
    - 8020426: 0x017ec98d0946bb0c008507871e3d865ec9a2dab6966b027d3c16aafa54407d4a

We have a split after block 8020425:
    - we drop:
        - 8020426: 0x017ec98d0946bb0c008507871e3d865ec9a2dab6966b027d3c16aafa54407d4a
    - we insert:
        - 8020426: 0xa388792fc1fa244083f7d5ad4a2843ac3ea0b23cc543078e1b18085f309fb44f
        - 8020427: 0xd6a8f9d468a7e2a916482211c0bf2fa0314178e2ef4979961a9e44d499046c86

From fixture we know - 0x017ec98d0946bb0c008507871e3d865ec9a2dab6966b027d3c16aafa54407d4a - is forked block

We know about differences in ether balances for blocks.
This is history data.

                account address                |         balances in fork       |  expected_balance in chain
    0xe75fe8be89d97101d1d84878bb876a1e6b12b83e |            3560031043197580749 | 3558388802072277453
    0xd87533f6450a125905e7d487910f2a12e75b2ef8 |             467861372000000000 | 467075624000000000
    0x85176612cc64c822a5e7a4746a9a764841378b8b |             319857145927679600 | 319026064927679600
    0x3462e5a279eb720d0ead2afc81a05e95b72ea9f2 |            1229374229878286171 | 1228876116878286171
    0xf056f435ba0cc4fcd2f1b17e3766549ffc404b94 |        28475095993859200000000 | 28492400979014200000000
    0xd8a83b72377476d0a66683cde20a8aad0b628713 |       103724185598784000000000 | 103726437107684000000000
    0x4c48aa89c93bbefe147dd4bfc499060659559875 |               7649952000000000 | 7572647000000000
    0x3c6e761fbbcdb9fa09179fe9eb07fc42138917b0 |              12706166442444081 | 12335502442444081
    0xb8fdba39c8d77ccaba086bf5315eaeebf4a62cfd |              27829325001162768 | 27793785001162768
    0x034f854b44d28e26386c1bc37ff9b20c6380b00d |        90280671904874700000000 | 90285403522674700000000
    0x9e839f7b0651060c37c45fcda022091c518bf00a |               8885800000000000 | 0
    0xb9a4873d8d2c22e56b8574e8605644d08e047549 |        14884337326811300000000 | 14896904943301300000000
    0x5ff87907d6157f18732ce912153149a3f9362a0b |            7326610699177916390 | 7328010699177916390
    0x61dbdc7a60a153084999ba57d9f836975463c7d2 |           20855834320000000000 | 20829534320000000000
    0x06b8c5883ec71bc3f4b332081519f23834c8706e |            8878835918307764627 | 8599859004137345165
    0xe0c1582a5cd193172624658ed0abeecea24835ad |           27670539114716587280 | 27669000514716587280
    0x6d78475812904b41c1c33259b76da553ca6ad4c4 |             302503749820064763 | 302181291820064763
"""

import pytest

from jsearch.common.structs import SyncRange
from jsearch.common.tables import assets_summary_t
from jsearch.syncer.database import RawDB, MainDB
from jsearch.syncer.manager import Manager


@pytest.fixture()
def mock_getting_last_block_from_row_db(mocker):
    async def get_last_block(*args):
        return None

    mocker.patch('jsearch.syncer.database.RawDB.get_latest_available_block_number', get_last_block)


async def call_system_under_test(db_dsn: MainDB, raw_db_dsn: str, start: int, end: int) -> None:
    async with MainDB(db_dsn) as main_db, RawDB(raw_db_dsn) as raw_db:
        manager = Manager(None, main_db, raw_db, sync_range=SyncRange(start=start, end=end))
        for i in range(0, 10):
            await manager.get_and_process_chain_event()


@pytest.mark.usefixtures('mock_getting_last_block_from_row_db')
async def test_chain_split_token_check_ether_summary_was_marked_as_forked(db, raw_db_split_sample, raw_db_dsn, db_dsn):
    # given
    total_forked_balances = 171
    total_inserted_balances = 338
    total_previous_balances = 122

    common_block_hash = '0xc612bbb64b83b23729ff542d7ced2617d028834681893a8ea28bb2dcfc01ddc9'
    forked_block_hash = '0x017ec98d0946bb0c008507871e3d865ec9a2dab6966b027d3c16aafa54407d4a'
    inserted_blocks_hashes = [
        '0xa388792fc1fa244083f7d5ad4a2843ac3ea0b23cc543078e1b18085f309fb44f',
        '0xd6a8f9d468a7e2a916482211c0bf2fa0314178e2ef4979961a9e44d499046c86',
    ]

    # when
    await call_system_under_test(db_dsn, raw_db_dsn, start=8020425, end=8020427)

    # then
    forked_balances = db.execute(
        assets_summary_t.select().where(assets_summary_t.c.block_hash == forked_block_hash)
    ).fetchall()

    assert len(forked_balances) == total_forked_balances
    assert all(x.is_forked for x in forked_balances)

    inserted_balances = db.execute(
        assets_summary_t.select().where(assets_summary_t.c.block_hash.in_(inserted_blocks_hashes))
    ).fetchall()

    assert len(inserted_balances) == total_inserted_balances
    assert all(not x.is_forked for x in inserted_balances)

    previous_balances = db.execute(
        assets_summary_t.select().where(assets_summary_t.c.block_hash == common_block_hash)
    ).fetchall()

    assert len(previous_balances) == total_previous_balances
    assert all(not x.is_forked for x in previous_balances)
