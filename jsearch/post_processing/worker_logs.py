import asyncio
import logging
import time
from itertools import groupby, chain
from typing import List

from jsearch import settings
from jsearch.common.processing.logs import process_log_event
from jsearch.multiprocessing import executor
from jsearch.post_processing.metrics import Metrics, Metric
from jsearch.service_bus import service_bus, WORKER_HANDLE_TRANSACTION_LOGS
from jsearch.syncer.database import MainDBSync
from jsearch.typing import Logs

metrics = Metrics()

logger = logging.getLogger(__name__)


@service_bus.listen_stream(WORKER_HANDLE_TRANSACTION_LOGS, task_limit=30, batch_size=10, batch_timeout=5)
async def handle_transaction_logs(blocks: List[Logs]):
    if not blocks:
        return

    loop = asyncio.get_event_loop()

    logs_per_seconds = Metric('logs_per_second')
    blocks_per_seconds = Metric('blocks_per_second')

    logs = list(chain(*blocks))
    transfers = await loop.run_in_executor(executor.get(), worker, logs)

    block_transfers = groupby(sorted(transfers, key=lambda x: x['block_number']), lambda x: x['block_number'])
    futures = []
    for block, items in block_transfers:
        block_transfers = list(items)
        future = await service_bus.send_transfers(value=block_transfers)
        futures.append(future)

    if futures:
        await asyncio.gather(*futures)

    logs_per_seconds.finish(value=len(logs))
    blocks_per_seconds.finish(value=1)

    metrics.update(logs_per_seconds)
    metrics.update(blocks_per_seconds)
    metrics.set_value(
        name='last_block',
        value=logs[0]['block_number'],
        callback=lambda prev, value: (prev and prev > value) or value
    )


def worker(logs: Logs) -> Logs:
    """
    Process transaction logs.
    Parse their event types and args via ERC20 ABI.
    Detect ERC20 transfers.

    Args:
        logs: list of transaction logs

    Returns:
        list of erc20 transfers
    """
    with MainDBSync(settings.JSEARCH_MAIN_DB) as db:
        start_at = time.time()
        logs = [process_log_event(log) for log in logs]
        for log in logs:
            db.update_log(
                tx_hash=log['transaction_hash'],
                log_index=log['log_index'],
                block_hash=log['block_hash'],
                values={
                    'is_processed': True,
                    'is_token_transfer': log.get('is_token_transfer', False),
                    'token_transfer_to': log.get('token_transfer_to'),
                    'token_transfer_from': log.get('token_transfer_from'),
                    'token_amount': log.get('token_amount'),
                    'event_type': log.get('event_type'),
                    'event_args': log.get('event_args'),
                }
            )
        logger.info('[WORKER] log update speed %0.2f', len(logs) / (time.time() - start_at))
    return [log for log in logs if log['is_token_transfer']]
