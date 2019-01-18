import asyncio
import logging
from itertools import count
from typing import Optional, List, Any, Dict

import asyncpg
from asyncpg.pool import Pool
from web3 import Web3

from jsearch import settings
from jsearch.common.rpc import eth_call_batch
from jsearch.validation.proxy import TokenProxy
from jsearch.validation.queries import (
    get_balances_sum,
    get_total_holders_count,
    get_total_positive_holders_count,
    get_total_transactions_count,
    iterate_holders,
    iterate_transfers,
    update_token_holder_balance,
)

logger = logging.getLogger(__name__)

QUERY_SIZE = 1000
BATCH_REQUEST_SIZE = 50
HOLDERS_PAGE_SIZE = 100


async def show_stats(token: TokenProxy) -> None:
    db_pool = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)

    holders_count = await get_total_positive_holders_count(pool=db_pool, token_address=token.address)
    total_holders_count = await get_total_holders_count(pool=db_pool, token_address=token.address)

    txs_count = await get_total_transactions_count(pool=db_pool, token_address=token.address)

    balances_sum = await get_balances_sum(pool=db_pool, token_address=token.address)

    print(f"[STATISTICS] Token holders with positive balance {total_holders_count}")
    print(f"[STATISTICS] Total token holders {holders_count}")
    print(f"[STATISTICS] Total token transactions {txs_count}")

    print(f"[STATISTICS] Balances sum {balances_sum}. Token total supply {token.total_supply}")


async def show_holders(token: TokenProxy, limit: Optional = None) -> None:
    db_pool = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)

    print(f"Address{' ' * 36 }| Actual value { ' ' * 17 } | Percent | Decimals")

    counter = count()
    async for holder in await iterate_holders(db_pool, token_address=token.address):
        address = holder["account_address"]
        balance = holder["balance"]
        decimals = holder["decimals"] or "-"

        percent = round(balance / token.total_supply * 100, 2)
        print(f"{address} | {balance:<30} | {percent:<4.2f} % | {decimals:<4}")

        if limit and next(counter) > limit:
            break


async def show_transfers(token: TokenProxy, limit: Optional = None) -> None:
    db_pool = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)

    counter = count()
    async for transfer in await iterate_transfers(db_pool, token_address=token.address):
        block_number = transfer['block_number']
        tx_hash = transfer['transaction_hash']
        from_ = transfer['token_transfer_from']
        to_ = transfer['token_transfer_to']
        value = transfer['token_amount']

        print(f"{block_number:<7} | {tx_hash}.. | {from_} | {to_} | {value:<30}")

        if limit and next(counter) > limit:
            break


async def check_token_holder_chunk(db_pool: Pool,
                                   token: TokenProxy,
                                   chunk: List[Dict[str, Any]],
                                   rewrite_invalid_values: bool = False) -> int:
    request_counter = count()

    accounts = [Web3.toChecksumAddress(item['accountAddress']) for item in chunk]
    calls = [token.get_balance_call(pk=next(request_counter), args=[account]) for account in accounts]

    results = eth_call_batch(calls=calls)
    balances = [results.get(call.pk) for call in calls]

    updates = list()
    errors = 0
    for original_balance, token_holder in zip(balances, chunk):
        address = token_holder['accountAddress']
        balance = token_holder['balance']

        if original_balance != balance:
            errors += 1
            print(f"{address} | {original_balance:<30} | {balance:<30}")

            if rewrite_invalid_values:
                update = update_token_holder_balance(
                    pool=db_pool,
                    token_address=token.address,
                    account_address=address,
                    balance=original_balance,
                    decimals=token.decimals
                )
                updates.append(update)

    if rewrite_invalid_values:
        await asyncio.gather(*updates)

    return errors


async def check_token_holder_balances(token: TokenProxy, rewrite_invalid_values=False) -> None:
    db_pool = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)
    total_records = await get_total_holders_count(db_pool, token.address)

    print(f"Address{' ' * 36 }| Actual value { ' ' * 17 } | Value in database ")

    errors = 0
    chunk = []
    processed = count(step=BATCH_REQUEST_SIZE)
    async for holder in await iterate_holders(db_pool, token_address=token.address):
        if len(chunk) < BATCH_REQUEST_SIZE:
            chunk.append(holder)
        else:
            errors += await check_token_holder_chunk(db_pool, token, chunk, rewrite_invalid_values)
            chunk = []
            progress = next(processed) / total_records * 100.0
            print(f"[PROGRESS] {progress:2.2f}")

    if chunk:
        errors += await check_token_holder_chunk(db_pool, token, chunk, rewrite_invalid_values)

    print(f"[STATISTICS] {total_records} total records with {errors} errors")
