from typing import AsyncGenerator, Dict, Any

from asyncpg.pool import Pool


async def iterate_query(pool: Pool, query: str, *args: Any, **kwargs: Any) -> AsyncGenerator[Dict[str, Any], None]:
    async with pool.acquire() as connection:
        async with connection.transaction():
            async for record in connection.cursor(query, *args, **kwargs):
                yield dict(record)


async def iterate_holders(pool: Pool, token_address: str) -> AsyncGenerator[Dict[str, Any], None]:
    query = """
        SELECT account_address, 
               token_address, 
               balance, 
               decimals
        FROM token_holders WHERE token_address = $1
        ORDER BY balance desc;
    """
    return iterate_query(pool, query, token_address)


async def iterate_transfers(pool: Pool, token_address: str) -> AsyncGenerator[Dict[str, Any], None]:
    query = """
        SELECT block_number,
               block_hash, 
               transaction_hash,
               token_amount,
               token_transfer_from,
               token_transfer_to
        FROM logs
        WHERE address = $1 AND is_token_transfer = true
        ORDER BY block_number asc;
    """
    return iterate_query(pool, query, token_address)


async def get_total_holders_count(pool: Pool, token_address: str) -> int:
    query = "SELECT count(*) as count FROM token_holders WHERE token_address = $1"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, token_address)
    return row['count']


async def get_total_positive_holders_count(pool: Pool, token_address: str) -> int:
    query = "SELECT count(*) as count FROM token_holders WHERE token_address = $1 AND balance > 0"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, token_address)
    return row['count']


async def get_total_transactions_count(pool: Pool, token_address: str) -> int:
    query = """
        SELECT count(*)
        FROM logs
        WHERE address = $1 AND is_token_transfer = true
    """

    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, token_address)
    return row['count']


async def update_token_holder_balance(pool: Pool,
                                      token_address: str,
                                      account_address: str,
                                      balance: int,
                                      decimals: int) -> None:
    query = """
        UPDATE token_holders
        SET balance = $3, decimals = $4
        WHERE account_address = $1 and token_address = $2;
    """
    async with pool.acquire() as conn:
        await conn.execute(query, account_address, token_address, balance, decimals)


async def get_balances_sum(pool: Pool, token_address: str) -> int:
    query = """
        SELECT sum(balance) as total_supply
        FROM token_holders
        WHERE token_address = $1
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, token_address)
    return row['total_supply']
