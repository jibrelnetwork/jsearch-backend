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
            row = await conn.fetchrow(query, address)
            if row is None:
                return None
            row = dict(row)
            del row['root']
            del row['storage']
            row['balance'] = int(row['balance'])
            return models.Account(**row)

    async def get_account_transactions(self, address):
        query = """SELECT * FROM transactions WHERE "to"=$1 OR "from"=$1 LIMIT 100"""

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address.lower())
            rows = [dict(r) for r in rows]
            return [models.Transaction(_from=r.pop('from'), **r) for r in rows]

    async def get_block_transactions(self, tag):
        if tag.is_hash():
            query = """SELECT * FROM transactions WHERE block_hash=$1"""
        elif tag.is_number():
            query = """SELECT * FROM transactions WHERE block_number=$1"""
        else:
            query = """SELECT * FROM transactions WHERE block_number=(SELECT max(block_number) FROM blocks)"""

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, tag.value)
            rows = [dict(r) for r in rows]
            return [models.Transaction(_from=r.pop('from'), **r) for r in rows]

    async def get_block(self, tag):

        if tag.is_hash():
            query = """SELECT * FROM blocks WHERE hash=$1"""
            tx_query = """SELECT hash FROM transactions WHERE block_hash=$1"""
        elif tag.is_number():
            query = """SELECT * FROM blocks WHERE number=$1"""
            tx_query = """SELECT hash FROM transactions WHERE block_number=$1"""
        else:
            query = """SELECT * FROM blocks WHERE number=(SELECT max(block_number) FROM blocks)"""
            tx_query = """SELECT hash FROM transactions WHERE block_number=(SELECT max(block_number) FROM blocks)"""

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, tag.value)
            if row is None:
                return None
            data = dict(row)
            del data['is_sequence_sync']
            txs = await conn.fetch(tx_query, tag.value)
            data['transactions'] = [tx['hash'] for tx in txs]
            return models.Block(**data)

    async def get_block_uncles(self, tag):
        if tag.is_hash():
            query = """SELECT * FROM uncles WHERE block_hash=$1"""
        elif tag.is_number():
            query = """SELECT * FROM uncles WHERE block_number=$1"""
        else:
            query = """SELECT * FROM uncles WHERE block_number=(SELECT max(block_number) FROM blocks)"""

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, tag.value)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['block_hash']
                del r['block_number']
            return [models.Uncle(**r) for r in rows]

    async def get_transaction(self, tx_hash):
        query = """SELECT * FROM transactions WHERE hash=$1"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, tx_hash)
            if row is None:
                return None
            row = dict(row)
            return models.Transaction(_from=row.pop('from'), **row)

    async def get_receipt(self, tx_hash):
        query = """SELECT * FROM receipts WHERE transaction_hash=$1"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, tx_hash)
            if row is None:
                return None
            row = dict(row)
            return models.Receipt(_from=row.pop('from'), **row)
