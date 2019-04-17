import asyncio
import logging
import time
from itertools import chain, groupby
from typing import List, Dict, Union

from jsearch import settings
from jsearch.common.last_block import LastBlock
from jsearch.common.processing.erc20_balances import update_token_holder_balances
from jsearch.common.processing.erc20_transfers import logs_to_transfers
from jsearch.common.processing.utils import fetch_contracts, prefetch_decimals
from jsearch.multiprocessing import executor
from jsearch.post_processing.metrics import Metrics, Metric
from jsearch.service_bus import service_bus, sync_client, ROUTE_HANDLE_LAST_BLOCK, ROUTE_HANDLE_ERC20_TRANSFERS
from jsearch.syncer.database import MainDBSync
from jsearch.typing import Contracts, Transfers, Logs

metrics = Metrics()

logger = logging.getLogger('worker')


async def write_transfers_to_bus(transfers: Transfers):
    transfers_by_blocks = groupby(
        iterable=sorted(transfers, key=lambda x: x['block_number']),
        key=lambda x: x['block_number']
    )

    for block, items in transfers_by_blocks:
        sync_client.write_transfers(items)


def worker(contracts: Contracts, transfer_logs: Logs, last_block: Union[int, str]) -> None:
    sync_client.start()
    with MainDBSync(settings.JSEARCH_MAIN_DB) as db:
        start_at = time.time()
        contracts = prefetch_decimals(contracts)

        block_hashes = list({log['block_hash'] for log in transfer_logs})
        blocks = {block['hash']: block for block in db.get_blocks(hashes=block_hashes)}

        transfers = logs_to_transfers(transfer_logs, blocks, contracts)
        db.insert_or_update_transfers(transfers)
        write_transfers_to_bus(transfers)
        logger.info(
            'Insert batch of token transfers',
            extra={
                'tag': 'WORKER',
                'average_speed': len(transfers) / (time.time() - start_at),
            },
        )

        start_at = time.time()
        update_token_holder_balances(db, transfers, contracts, last_block)
        logger.info(
            'Updated batch of token holder balances',
            extra={
                'tag': 'WORKER',
                'average_speed': len(transfers) / (time.time() - start_at),
            },
        )


@service_bus.listen_stream(ROUTE_HANDLE_ERC20_TRANSFERS, task_limit=30, batch_size=20, batch_timeout=5)
async def handle_new_transfers(blocks: List[Transfers]):
    loop = asyncio.get_event_loop()
    last_block = LastBlock()

    metric_logs = Metric('logs')
    metric_blocks = Metric('blocks')

    logs = list(chain(*blocks))
    last_stable_block = await last_block.get()

    addresses = list({log['address'] for log in logs})
    contracts = await fetch_contracts(addresses)

    await loop.run_in_executor(executor.get(), worker, contracts, logs, last_stable_block)

    metric_logs.finish(value=len(logs))
    metric_blocks.finish(value=len(blocks))

    metrics.update(metric_logs)
    metrics.update(metric_blocks)
    metrics.set_value(
        name='last_block',
        value=logs[0]['block_number'],
        is_need_to_update=lambda prev, value: prev is None or prev < value
    )


@service_bus.listen_stream(ROUTE_HANDLE_LAST_BLOCK)
async def receive_last_block(record: Dict[str, int]):
    number = record.get('number')

    logger.info("Received new last block", extra={'tag': 'LAST BLOCK', 'number': number})

    last_block = LastBlock()
    last_block.update(number=number)
