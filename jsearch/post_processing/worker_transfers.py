import asyncio
import logging
from itertools import chain
from typing import List

from jsearch import settings
from jsearch.common.last_block import LastBlock
from jsearch.common.processing.erc20_balances import update_token_holder_balances
from jsearch.common.processing.utils import fetch_contracts, prefetch_decimals
from jsearch.multiprocessing import executor
from jsearch.post_processing.metrics import Metrics, Metric
from jsearch.service_bus import service_bus, ROUTE_HANDLE_ERC20_TRANSFERS, ROUTE_HANDLE_LAST_BLOCK
from jsearch.syncer.database import MainDBSync
from jsearch.typing import Contracts, Transfers

metrics = Metrics()

logger = logging.getLogger('worker')


@service_bus.listen_stream(ROUTE_HANDLE_ERC20_TRANSFERS, task_limit=30, batch_size=20, batch_timeout=5)
async def handle_new_transfers(blocks: List[Transfers]):
    loop = asyncio.get_event_loop()
    last_block = LastBlock()

    logs_per_seconds = Metric('logs_per_second')
    blocks_per_seconds = Metric('blocks_per_second')

    logs = list(chain(*blocks))
    last_stable_block = await last_block.get_last_stable_block()

    addresses = list({log['token_address'] for log in logs})
    contracts = await fetch_contracts(addresses)

    await loop.run_in_executor(executor.get(), worker, contracts, logs, last_stable_block)

    logs_per_seconds.finish(value=len(logs))
    blocks_per_seconds.finish(value=len(blocks))

    metrics.update(logs_per_seconds)
    metrics.update(blocks_per_seconds)
    metrics.set_value(
        name='last_block',
        value=logs[0]['block_number'],
        callback=lambda prev, value: (prev and prev > value) or value
    )


@service_bus.listen_stream(ROUTE_HANDLE_LAST_BLOCK)
async def receive_last_block(number):
    logger.info("[LAST BLOCK] Receive new last block number %s", number)

    last_block = LastBlock()
    last_block.update(number=number)


def worker(contracts: Contracts, transfers: Transfers, last_block: int) -> None:
    with MainDBSync(settings.JSEARCH_MAIN_DB) as db:
        contracts = prefetch_decimals(contracts)
        update_token_holder_balances(db, transfers, contracts, last_block)
