from jsearch.api import models


class Storage:

    def __init__(self, pool):
        self.pool = pool

    async def get_account(self, address):
        """
        Get account info by address
        """
        query = """SELECT * FROM accounts WHERE address=$1 LIMIT 1"""

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(query, address)
                if row is None:
                    return None
                return models.Account.from_row(row)

    async def get_account_transactions(self, address):
        query = """SELECT * FROM transactions WHERE fields->'to'=$1 LIMIT 100"""

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(query, '"{}"'.format(address.lower()))
                return [models.Transaction.from_row(r) for r in rows]

    async def get_block(self, hash=None, number=None):
        assert hash is not None or number is not None, 'Hash or number required'

        if hash is not None:
            query = """SELECT * FROM blocks WHERE hash=$1"""
            tx_query = """SELECT hash FROM transactions WHERE block_hash=$1"""
            arg = hash
        else:
            query = """SELECT * FROM blocks WHERE number=$1"""
            tx_query = """SELECT hash FROM transactions WHERE block_number=$1"""
            arg = number

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, arg)
            if row is None:
                return None
            data = dict(row)
            del data['is_sequence_sync']
            txs = await conn.fetch(tx_query, arg)
            data['transactions'] = [tx['hash'] for tx in txs]
            return models.Block(**data)
