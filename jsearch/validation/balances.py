import asyncio
import logging
from functools import partial
from itertools import count

import asyncpg
from web3 import Web3

from jsearch import settings
from jsearch.api.storage import Storage
from jsearch.common.rpc import ContractCall, eth_call_batch, eth_call
from jsearch.typing import Token
from jsearch.utils import split
from jsearch.validation.queries import (
    get_total_transactions_count,
    get_total_holders_count,
    update_token_holder_balance
)

logger = logging.getLogger(__name__)

QUERY_SIZE = 1000
BATCH_REQUEST_SIZE = 50


async def show_statistics(token):
    db_pool = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)

    holders_count = await get_total_holders_count(pool=db_pool, token_address=token['address'])
    txs_count = await get_total_transactions_count(pool=db_pool, token_address=token['address'])

    logging.info(f"[STATISTICS] Total token holders %s", holders_count)
    logging.info(f"[STATISTICS] Total token transactions %s", txs_count)


async def check_token_holder_balances(token: Token, rewrite_invalide_values=False) -> None:
    db_pool = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)
    storage = Storage(pool=db_pool)

    token_abi = token['abi']
    token_address = token['address']
    token_address_checksum = Web3.toChecksumAddress(token_address)

    token_call = partial(ContractCall, abi=token_abi, address=token_address_checksum)

    get_balance = partial(token_call, method='balanceOf')
    get_decimals = partial(token_call, method='decimals')

    token_decimals = eth_call(call=get_decimals())

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

            updates = list()
            for original_balance, token_holder in zip(balances, chunk):
                address = token_holder['accountAddress']
                balance = token_holder['balance']

                if original_balance != balance:
                    errors += 1
                    print(f"{address}: {original_balance} != {balance}")

                    if rewrite_invalide_values:
                        update = update_token_holder_balance(
                            pool=db_pool,
                            token_address=token_address,
                            account_address=address,
                            balance=original_balance,
                            decimals=token_decimals
                        )
                        updates.append(update)
                else:
                    logging.debug(f"%s: %s == %s", address, original_balance, balance)

            if rewrite_invalide_values:
                await asyncio.gather(*updates)

        if offset:
            print(f"[PROGRESS] {round(offset / total_records * 100, 2)}")

    print(f"[STATISTICS] {total_records} total records with {errors} errors")
