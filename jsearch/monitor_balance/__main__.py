# !/usr/bin/env python
import asyncio
import logging
import os

import click
from aiopg.sa import create_engine, SAConnection
from typing import Optional

from jsearch import settings
from jsearch.common import logs
from jsearch.common.processing.erc20_balances import get_balances
from jsearch.monitor_balance.queries import (
    update_balance,
    get_token_holders_balances,
    get_token_holders_from_empty_assets_summary,
    get_last_block
)

logger = logging.getLogger(__name__)

DEFAULT_OFFSET = 6


def get_offset():
    offset = os.environ['MONITOR_OFFSET']
    logging.info('Set settings', extra={'offset': offset})
    return int(offset)


async def check_and_fix_balances(connection: SAConnection, token_holders_balances, block, force=False) -> None:
    token_holders = list(token_holders_balances.keys())
    balances = dict(await get_balances(token_holders, settings.ETH_NODE_BATCH_REQUEST_SIZE, block))

    for token_holder in token_holders:
        expected_balance = balances[token_holder]
        original_balance = token_holders_balances[token_holder]

        if original_balance != expected_balance or force:
            logging.info(
                f"Need to update",
                extra={
                    "block": block,
                    "token": token_holder.token,
                    "account": token_holder.account,
                    "origin": original_balance,
                    "expected": expected_balance
                }
            )
            await update_balance(connection, token_holder, expected_balance, block)


async def check_balance_on_block(connection: SAConnection, block: int) -> None:
    token_holders_balances = await get_token_holders_balances(connection, block)
    await check_and_fix_balances(connection, token_holders_balances, block)


async def check_and_fix_empty_blocks_in_assets_summary(connection: SAConnection, block: int) -> int:
    token_holders_balances = await get_token_holders_from_empty_assets_summary(connection)
    await check_and_fix_balances(connection, token_holders_balances, block, force=True)
    return len(token_holders_balances)


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


async def fix_empty_blocks_assets_summary(offset: int) -> None:
    async with create_engine(settings.JSEARCH_MAIN_DB) as engine, create_engine(settings.JSEARCH_RAW_DB) as raw_engine:
        async with raw_engine.acquire() as connection:
            block = await get_start_block(connection, offset=offset)

        is_need_to_stop = False
        while not is_need_to_stop:
            try:
                async with raw_engine.acquire() as connection:
                    last_block = await get_last_block(connection)

                async with engine.acquire() as connection:
                    updates = await check_and_fix_empty_blocks_in_assets_summary(connection, block=last_block - offset)
                    logging.info(
                        "There are records with empty block_number in assets_summary...",
                        extra={"count": updates}
                    )
                    if not updates:
                        logging.info(
                            "There are not empty blocks records in assets_summary...",
                            extra={"block": block, "last_block": last_block, "timeout": 15}
                        )
                        is_need_to_stop = True

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
    loop.run_until_complete(
        asyncio.gather(
            check_balances(block=block, offset=offset),
            fix_empty_blocks_assets_summary(offset=offset)
        )
    )


if __name__ == '__main__':
    main()
