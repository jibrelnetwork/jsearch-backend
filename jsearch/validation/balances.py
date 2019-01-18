import asyncio
import logging
from itertools import count
from typing import Optional

import asyncpg
from web3 import Web3

from jsearch import settings
from jsearch.api.storage import Storage
from jsearch.common.rpc import eth_call_batch
from jsearch.utils import split
from jsearch.validation.proxy import TokenProxy
from jsearch.validation.queries import (
    get_total_transactions_count,
    get_total_holders_count,
    update_token_holder_balance,
    get_total_positive_holders_count,
    get_balances_sum,
    iterate_holders)

logger = logging.getLogger(__name__)

QUERY_SIZE = 1000
BATCH_REQUEST_SIZE = 50
HOLDERS_PAGE_SIZE = 100


async def show_statistics(token: TokenProxy) -> None:
    db_pool = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)

    holders_count = await get_total_positive_holders_count(pool=db_pool, token_address=token.address)
    total_holders_count = await get_total_holders_count(pool=db_pool, token_address=token.address)

    txs_count = await get_total_transactions_count(pool=db_pool, token_address=token.address)

    balances_sum = await get_balances_sum(pool=db_pool, token_address=token.address)

    print(f"[STATISTICS] Token holders with positive balance {total_holders_count}")
    print(f"[STATISTICS] Total token holders {holders_count}")
    print(f"[STATISTICS] Total token transactions {txs_count}")

    print(f"[STATISTICS] Balances sum {balances_sum}. Token total supply {token.total_supply}")


async def show_top_holders(token: TokenProxy, limit: Optional = None) -> None:
    db_pool = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)

    print(f"Address{' ' * 36 }| Actual value { ' ' * 17 } | Percent | Decimals")

    async for holder in iterate_holders(db_pool, token_address=token.address):
        address = holder["account_address"]
        balance = holder["balance"]
        decimals = holder["decimals"] or "-"

        percent = round(balance / token.total_supply * 100, 2)
        print(f"{address} | {balance:<30} | {percent:<4.2f} % | {decimals:<4}")


async def check_token_holder_balances(token: TokenProxy, rewrite_invalide_values=False) -> None:
    db_pool = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)
    storage = Storage(pool=db_pool)

    errors = 0

    total_records = await get_total_holders_count(pool=db_pool, token_address=token.address)
    print(f"Address{' ' * 36 }| Actual value { ' ' * 17 } | Value in database ")
    for offset in range(0, total_records, QUERY_SIZE):
        holders = await storage.get_tokens_holders(address=token.address, offset=offset, limit=QUERY_SIZE, order='asc')
        holders = [holder.to_dict() for holder in holders]

        for chunk in split(holders, size=BATCH_REQUEST_SIZE):
            counter = count()

            accounts = [Web3.toChecksumAddress(item['accountAddress']) for item in chunk]
            calls = [token.get_balance_call(pk=next(counter), args=[account]) for account in accounts]

            results = eth_call_batch(calls=calls)
            balances = [results.get(call.pk) for call in calls]

            updates = list()
            for original_balance, token_holder in zip(balances, chunk):
                address = token_holder['accountAddress']
                balance = token_holder['balance']

                if original_balance != balance:
                    errors += 1
                    print(f"{address} | {original_balance:<30} | {balance:<30}")

                    if rewrite_invalide_values:
                        update = update_token_holder_balance(
                            pool=db_pool,
                            token_address=token.address,
                            account_address=address,
                            balance=original_balance,
                            decimals=token.decimals
                        )
                        updates.append(update)

            if rewrite_invalide_values:
                await asyncio.gather(*updates)

        if offset:
            progress = offset / total_records * 100.0
            print(f"[PROGRESS] {progress:2.2f}")

    print(f"[STATISTICS] {total_records} total records with {errors} errors")
