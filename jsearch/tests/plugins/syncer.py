import async_timeout

import pytest


@pytest.fixture(scope="module")
@pytest.mark.asyncio
async def sync(event_loop, main_db_wrapper, raw_db_wrapper):
    from jsearch.syncer.manager import Manager
    from jsearch.common.structs import BlockRange

    async def start_syncer(block_range: BlockRange, node_id: str = ""):
        async with async_timeout.timeout(3000):
            manager = Manager(
                service=None,
                main_db=main_db_wrapper,
                raw_db=raw_db_wrapper,
                sync_range=block_range,
            )
            manager._running = True
            manager.node_id = node_id
            await manager.chain_events_process_loop()

    yield start_syncer
