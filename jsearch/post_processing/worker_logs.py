import asyncio
import logging
import time
from itertools import groupby, chain
from typing import List

from jsearch import settings
from jsearch.common.processing.logs import process_log_event
from jsearch.multiprocessing import executor
from jsearch.post_processing.metrics import Metrics, Metric
from jsearch.service_bus import service_bus, ROUTE_HANDLE_TRANSACTION_LOGS
from jsearch.syncer.database import MainDBSync
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
    with MainDBSync(settings.JSEARCH_MAIN_DB) as db:
        start_at = time.time()
        for log in logs:
            log = process_log_event(log)
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

        logger.info(
            'Updated batch of logs',
            extra={
                'tag': 'WORKER',
                'average_speed': len(logs) / (time.time() - start_at),
            },
        )

        return [item for item in logs if item['is_token_transfer']]


async def write_transfers_logs_bus(transfer_logs: Logs):
    transfers_by_blocks = groupby(sorted(transfer_logs, key=lambda x: x['block_number']), lambda x: x['block_number'])
    futures = []
    for block, items in transfers_by_blocks:
        transfers_by_blocks = list(items)
        future = await service_bus.send_transfers(value=transfers_by_blocks)
        futures.append(future)

    if futures:
        await asyncio.gather(*futures)


@service_bus.listen_stream(ROUTE_HANDLE_TRANSACTION_LOGS, task_limit=30, batch_size=10, batch_timeout=5)
async def handle_transaction_logs(blocks: List[Logs]):
    if not blocks:
        return

    loop = asyncio.get_event_loop()

    metric_logs = Metric('logs')
    metric_blocks = Metric('blocks')

    logs = list(chain(*blocks))
    transfer_logs = await loop.run_in_executor(executor.get(), process_tx_logs, logs)

    await write_transfers_logs_bus(transfer_logs)

    metric_logs.finish(value=len(logs))
    metric_blocks.finish(value=1)

    metrics.update(metric_logs)
    metrics.update(metric_blocks)
    metrics.set_value(
        name='last_block',
        value=logs[0]['block_number'],
        is_need_to_update=lambda prev, value: prev is None or prev < value
    )
