# !/usr/bin/env python
import asyncio
import logging
import os

import click
from aiopg.sa import create_engine, SAConnection
from typing import Dict, Optional

from jsearch import settings
from jsearch.common import logs
from jsearch.common.processing.erc20_balances import get_balances
from jsearch.common.tables import token_holders_t
from jsearch.syncer.database_queries.assets_summary import upsert_assets_summary_query
from jsearch.syncer.database_queries.token_holders import upsert_token_holder_balance_q
from jsearch.syncer.structs import TokenHolder
from jsearch.syncer.utils import report_erc20_balance_error, insert_balance_request

logger = logging.getLogger(__name__)

DEFAULT_OFFSET = 6


def get_offset():
    offset = os.getenv('MONITOR_OFFSET', DEFAULT_OFFSET)
    logging.info('Set settings', extra={'offset': offset})
    return int(offset)


async def get_last_block(connection: SAConnection) -> int:
    query = "SELECT block_number FROM bodies ORDER BY block_number DESC LIMIT 1"

    async with connection.execute(query) as cursor:
        result = await cursor.fetchone()
        if result:
            return result.block_number


async def update_balance(connection: SAConnection, token_holder: TokenHolder, balance: int, block: int) -> None:
    upsert_token_holder = upsert_token_holder_balance_q(
        token_address=token_holder.token,
        account_address=token_holder.account,
        balance=balance,
        block_number=block,
    )

    upsert_assets_summary = upsert_assets_summary_query(
        address=token_holder.account,
        asset_address=token_holder.token,
        value=balance,
        blocks_to_replace=[block]
    )

    await connection.execute(upsert_token_holder)
    await connection.execute(upsert_assets_summary)


async def get_token_holders_balances(connection: SAConnection, block: int) -> Dict[TokenHolder, int]:
    query = token_holders_t.select().where(token_holders_t.c.block_number == block)
    async with connection.execute(query) as cursor:
        result = await cursor.fetchall()
        return {TokenHolder(token=x.token_address, account=x.account_address): x.balance for x in result}


async def check_balance_on_block(connection: SAConnection, block: int) -> None:
    token_holders_balances = await get_token_holders_balances(connection, block)
    token_holders = list(token_holders_balances.keys())

    balances = dict(await get_balances(token_holders, settings.ETH_NODE_BATCH_REQUEST_SIZE, block))

    async with connection.begin():
        for token_holder in token_holders:
            expected_balance = balances[token_holder]
            original_balance = token_holders_balances[token_holder]

            if original_balance != expected_balance:
                logging.info(
                    f"Invalid balance",
                    extra={
                        "block": block,
                        "token": token_holder.token,
                        "account": token_holder.account,
                        "origin": original_balance,
                        "expected": expected_balance
                    }
                )
                await report_erc20_balance_error(connection, token_holder.token, token_holder.account, block)
                await update_balance(connection, token_holder, expected_balance, block)
            await insert_balance_request(connection, token_holder.token, token_holder.account, expected_balance, block)


async def get_start_block(connection: SAConnection, offset: Optional[int] = None):
    last_block = await get_last_block(connection)
    if last_block is None:
        raise ValueError('Last available block is None')
    block = last_block - offset
    logging.info('Choose start block from offset', extra={'last_block': last_block, 'offset': offset})

    return block


async def check_balances(offset: int, block: Optional[int] = None) -> None:
    async with create_engine(settings.JSEARCH_MAIN_DB) as engine, create_engine(settings.JSEARCH_RAW_DB) as raw_engine:
        if block is None:
            async with raw_engine.acquire() as connection:
                block = await get_start_block(connection, offset=offset)

        while True:
            try:
                async with raw_engine.acquire() as connection:
                    last_block = await get_last_block(connection)

                logging.info(f'Block {block} - {last_block}')

                async with engine.acquire() as connection:

                    if block < last_block - offset:
                        await check_balance_on_block(connection, block)
                        block += 1
                    else:
                        logging.info("Waiting...", extra={"block": block, "last_block": last_block, "timeout": 15})
                        await asyncio.sleep(15)

            except KeyboardInterrupt:
                pass


@click.command()
@click.option('--block', type=int)
@click.option('--offset', type=int, help="Offset for node requests")
@click.option('--log-level', default=settings.LOG_LEVEL, help="Log level")
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
def main(
        block: Optional[int],
        offset: Optional[int],
        log_level: str,
        no_json_formatter: bool,
) -> None:
    offset = offset or get_offset()
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_balances(block=block, offset=offset))


if __name__ == '__main__':
    main()
