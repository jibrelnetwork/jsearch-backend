import asyncio
from itertools import chain
from typing import List

from jsearch import settings
from jsearch.common.processing.erc20_balances import (
    fetch_blocks,
    fetch_contracts,
    prefetch_decimals,
    process_log_operations_bulk,
)
from jsearch.common.processing.erc20_transfers import logs_to_transfers
from jsearch.multiprocessing import get_executor
from jsearch.post_processing.metrics import Metrics, Metric
from jsearch.service_bus import service_bus
from jsearch.syncer.database import MainDBSync
from jsearch.typing import Logs, Contracts

metrics = Metrics()


@service_bus.listen_stream('handle_erc20_transfers', batch_size=100, batch_timeout=5)
async def handle_new_transfers(blocks: List[Logs]):
    loop = asyncio.get_event_loop()
    executor = get_executor()

    logs_per_seconds = Metric('logs_per_second')
    blocks_per_seconds = Metric('blocks_per_second')

    logs = list(chain(*blocks))
    contracts = await fetch_contracts(service_bus, logs)
    await loop.run_in_executor(executor, worker, contracts, logs)

    logs_per_seconds.finish(value=len(logs))
    blocks_per_seconds.finish(value=len(blocks))

    metrics.update(logs_per_seconds)
    metrics.update(blocks_per_seconds)


def worker(contracts: Contracts, logs: Logs) -> None:
    with MainDBSync(settings.JSEARCH_MAIN_DB) as db:
        contracts = prefetch_decimals(contracts)
        logs = process_log_operations_bulk(db, logs, contracts)

        blocks = fetch_blocks(db, logs)
        transfers = logs_to_transfers(logs, blocks, contracts)

        db.insert_or_update_transfers(transfers)
