import asyncio
import logging

import time
from itertools import groupby, chain
from typing import List

from jsearch.common.processing.logs import process_log_event
from jsearch.multiprocessing import executor
from jsearch.post_processing.database import db_service
from jsearch.post_processing.metrics import Metrics, Metric
from jsearch.service_bus import service_bus, ROUTE_HANDLE_TRANSACTION_LOGS
from jsearch.typing import Logs

metrics = Metrics()

logger = logging.getLogger(__name__)


def process_tx_logs(logs: Logs) -> Logs:
    """
    Process transaction logs.
    Parse their event types and args via ERC20 ABI.
    Detect ERC20 transfers and send them to service bus.

    Args:
        logs: list of transaction logs

    Returns:
        list of erc20 transfers
    """
    start_at = time.time()
    logs = [process_log_event(log) for log in logs]
    logger.info(
        'Updated batch of logs',
        extra={
            'tag': 'WORKER',
            'average_speed': len(logs) / (time.time() - start_at),
        },
    )
    return logs


async def write_transfers_logs_to_bus(logs: Logs) -> None:
    transfer_logs = [item for item in logs if item['is_token_transfer']]
    transfers_by_blocks = groupby(
        iterable=sorted(transfer_logs, key=lambda x: x['block_number']),
        key=lambda x: x['block_number']
    )
    futures = []
    for block, items in transfers_by_blocks:
        transfers_by_blocks = list(items)
        future = service_bus.send_transfers(value=transfers_by_blocks)
        futures.append(future)

    if futures:
        await asyncio.gather(*futures)


@service_bus.listen_stream(
    ROUTE_HANDLE_TRANSACTION_LOGS,
    task_limit=30, batch_size=10, batch_timeout=1,
    service_name='jsearch_post_processing_logs'
)
async def handle_transaction_logs(blocks: List[Logs]):
    if blocks:
        loop = asyncio.get_event_loop()

        metric_logs = Metric('logs')
        metric_blocks = Metric('blocks')

        logs = list(chain(*blocks))
        logs = await loop.run_in_executor(executor.get(), process_tx_logs, logs)

        await db_service.update_logs(logs)
        await write_transfers_logs_to_bus(logs)

        metric_logs.finish(value=len(logs))
        metric_blocks.finish(value=1)

        metrics.update(metric_logs)
        metrics.update(metric_blocks)
        metrics.set_value(
            name='last_block',
            value=logs[0]['block_number'],
            is_need_to_update=lambda prev, value: prev is None or prev < value
        )
