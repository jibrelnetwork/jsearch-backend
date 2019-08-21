# !/usr/bin/env python

from aiopg.sa import SAConnection
from sqlalchemy import null, and_
from typing import Dict

from jsearch.common.tables import token_holders_t, assets_summary_t
from jsearch.syncer.database_queries.assets_summary import upsert_assets_summary_query
from jsearch.syncer.database_queries.token_holders import upsert_token_holder_balance_q
from jsearch.syncer.structs import TokenHolder


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
        block_number=block
    )

    await connection.execute(upsert_token_holder)
    await connection.execute(upsert_assets_summary)


async def get_token_holders_balances(connection: SAConnection, block: int) -> Dict[TokenHolder, int]:
    query = token_holders_t.select().where(token_holders_t.c.block_number == block)
    async with connection.execute(query) as cursor:
        result = await cursor.fetchall()
        return {TokenHolder(token=x.token_address, account=x.account_address): x.balance for x in result}


async def get_token_holders_from_empty_assets_summary(connection: SAConnection, limit=1000) -> Dict[TokenHolder, int]:
    query = assets_summary_t.select().where(
        and_(
            assets_summary_t.c.asset_address != '',
            assets_summary_t.c.block_number.is_(null())
        )
    ).limit(limit)
    async with connection.execute(query) as cursor:
        result = await cursor.fetchall()
    return {TokenHolder(token=x.asset_address, account=x.address): x.value for x in result if x.asset_address}
