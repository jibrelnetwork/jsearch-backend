from functools import partial
from itertools import groupby

from sqlalchemy import select, between, and_, true

from jsearch import settings
from jsearch.common.tables import logs_t
from jsearch.service_bus import ROUTE_HANDLE_TRANSACTION_LOGS
from jsearch.service_bus import service_bus
from jsearch.syncer.database import MainDB


def get_logs_query(block_from: int, block_until: int):
    return select(
        columns=[logs_t],
        whereclause=and_(
            between(logs_t.c.block_number, block_from, block_until)
        )
    ) \
        .order_by(logs_t.c.block_number.asc())


def get_transfers_query(block_from: int, block_until: int):
    return select(
        columns=[logs_t],
        whereclause=and_(
            logs_t.c.is_token_transfer == true(),
            between(logs_t.c.block_number, block_from, block_until)
        )
    ) \
        .order_by(logs_t.c.block_number.asc())


async def send_value_to_reprocess(get_query, block_from, block_until, step) -> None:
    i = block_from
    async with MainDB(settings.JSEARCH_MAIN_DB) as db:
        while not block_until or i < block_until:
            logs = await db.fetch_all(query=get_query(i, i + step))
            logs = [dict(log) for log in logs]
            if logs:
                for _, items in groupby(logs, lambda x: x['block_number']):
                    log_chunk = list(items)
                    await service_bus.send_to_stream(ROUTE_HANDLE_TRANSACTION_LOGS, log_chunk)

                i += step
            else:
                break


send_trx_logs_to_reprocess = partial(send_value_to_reprocess, get_logs_query)
send_erc20_transfers_to_reprocess = partial(send_value_to_reprocess, get_transfers_query)
