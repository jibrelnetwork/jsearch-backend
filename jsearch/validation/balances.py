import logging
from contextlib import suppress
from functools import partial
from itertools import count

import asyncpg
from web3 import Web3

from jsearch import settings
from jsearch.api.storage import Storage
from jsearch.common.rpc import ContractCall, eth_call_batch
from jsearch.typing import Token
from jsearch.utils import split

logger = logging.getLogger(__name__)

QUERY_SIZE = 1000
BATCH_REQUEST_SIZE = 50


def apply_decimals(value, decimals):
    with suppress(Exception):
        return value * (10 ** decimals)


async def get_total_holders_count(pool, token_address: str) -> int:
    query = "SELECT count(*) as count FROM token_holders WHERE token_address = $1"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, token_address)
    return row['count']


async def get_total_transactions_count(pool, token_address: str) -> int:
    query = f"""
        SELECT count(*)
        FROM logs
        WHERE address = $1 AND is_token_transfer = true
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, token_address)
    return row['count']


async def show_statistics(token):
    db_pool = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)

    holders_count = await get_total_holders_count(pool=db_pool, token_address=token['address'])
    txs_count = await get_total_transactions_count(pool=db_pool, token_address=token['address'])

    logging.info(f"[STATISTICS] Total token holders %s", holders_count)
    logging.info(f"[STATISTICS] Total token transactions %s", txs_count)


async def check_token_holder_balances(token: Token) -> None:
    db_pool = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)
    storage = Storage(pool=db_pool)

    token_abi = token['abi']
    token_address = Web3.toChecksumAddress(token['address'])

    token_call = partial(ContractCall, abi=token_abi, address=token_address)
    get_balance = partial(token_call, method='balanceOf')

    errors = 0

    total_records = await get_total_holders_count(pool=db_pool, token_address=token['address'])
    for offset in range(0, total_records, QUERY_SIZE):
        holders = await storage.get_tokens_holders(
            address=token['address'],
            offset=offset,
            limit=QUERY_SIZE,
            order='asc'
        )
        holders = [holder.to_dict() for holder in holders]

        for chunk in split(holders, size=BATCH_REQUEST_SIZE):
            counter = count()

            accounts = [Web3.toChecksumAddress(item['accountAddress']) for item in chunk]
            calls = [get_balance(pk=next(counter), args=[account]) for account in accounts]

            results = eth_call_batch(calls=calls)
            balances = [results.get(call.pk) for call in calls]

            for original_balance, item in zip(balances, chunk):
                address = item['accountAddress']
                balance = item['balance']

                if original_balance != balance:
                    errors += 1
                    print(f"{address}: {original_balance} != {balance}")
                else:
                    logging.debug(f"%s: %s == %s", address, original_balance, balance)

        if offset:
            print(f"[PROGRESS] {round(offset / total_records * 100, 2)}")

    print(f"[STATISTICS] {total_records} total records with {errors} errors")
