from jsearch.common.db import DBWrapper

MAIN_DB_POOL_SIZE = 1


class MainDB(DBWrapper):
    """
    jSearch Main db wrapper
    """
    pool_size = MAIN_DB_POOL_SIZE

    async def get_latest_synced_block_number(self) -> int:
        """
        Get latest block writed in main DB
        """
        q = """
            SELECT max(number) as max_number
            FROM blocks
            WHERE is_forked=false
        """
        async with self.engine.acquire() as conn:
            res = await conn.execute(q)
            row = await res.fetchone()
        return row and row['max_number'] or 0
