from jsearch.api import models


DEFAULT_ACCOUNT_TRANSACTIONS_LIMIT = 20
MAX_ACCOUNT_TRANSACTIONS_LIMIT = 200


class Storage:

    def __init__(self, pool):
        self.pool = pool

    async def get_account(self, address, tag):
        """
        Get account info by address
        """
        if tag.is_hash():
            query = """SELECT * FROM accounts WHERE address=$1 AND block_number<=(SELECT number FROM blocks WHERE hash=$2) ORDER BY block_number LIMIT 1"""
        elif tag.is_number():
            query = """SELECT * FROM accounts WHERE address=$1 AND block_number<=$2 ORDER BY block_number LIMIT 1"""
        else:
            query = """SELECT * FROM accounts WHERE address=$1 ORDER BY block_number LIMIT 1"""
        async with self.pool.acquire() as conn:
            if tag.is_latest():
                row = await conn.fetchrow(query, address)
            else:
                row = await conn.fetchrow(query, address, tag.value)
            if row is None:
                return None
            row = dict(row)
            del row['root']
            del row['storage']
            row['balance'] = int(row['balance'])
            return models.Account(**row)

    async def get_account_transactions(self, address, limit, offset):
        query = """SELECT * FROM transactions WHERE "to"=$1 OR "from"=$1 ORDER BY block_number, transaction_index LIMIT $2 OFFSET $3"""

        limit = min(limit, MAX_ACCOUNT_TRANSACTIONS_LIMIT)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address.lower(), limit, offset)
            rows = [dict(r) for r in rows]
            return [models.Transaction(**r) for r in rows]

    async def get_block_transactions(self, tag):
        if tag.is_hash():
            query = """SELECT * FROM transactions WHERE block_hash=$1"""
        elif tag.is_number():
            query = """SELECT * FROM transactions WHERE block_number=$1"""
        else:
            query = """SELECT * FROM transactions WHERE block_number=(SELECT max(number) FROM blocks)"""
        async with self.pool.acquire() as conn:
            if tag.is_latest():
                rows = await conn.fetch(query)
            else:
                rows = await conn.fetch(query, tag.value)
            rows = [dict(r) for r in rows]
            return [models.Transaction(**r) for r in rows]

    async def get_block(self, tag):

        if tag.is_hash():
            query = """SELECT * FROM blocks WHERE hash=$1"""
        elif tag.is_number():
            query = """SELECT * FROM blocks WHERE number=$1"""
        else:
            query = """SELECT * FROM blocks WHERE number=(SELECT max(number) FROM blocks)"""

        tx_query = """SELECT hash FROM transactions WHERE block_number=$1 ORDER BY transaction_index"""
        async with self.pool.acquire() as conn:
            if tag.is_latest():
                row = await conn.fetchrow(query)
            else:
                row = await conn.fetchrow(query, tag.value)
            if row is None:
                return None
            data = dict(row)
            del data['is_sequence_sync']

            data['static_reward'] = int(data['static_reward'])
            data['uncle_inclusion_reward'] = int(data['uncle_inclusion_reward'])
            data['tx_fees'] = int(data['tx_fees'])

            txs = await conn.fetch(tx_query, data['number'])
            data['transactions'] = [tx['hash'] for tx in txs]
            data['uncles'] = None
            return models.Block(**data)

    async def get_blocks(self, limit, offset, order):
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = """SELECT * FROM blocks ORDER BY number {} LIMIT $1 OFFSET $2""".format(order)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, limit, offset)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['is_sequence_sync']

                r['static_reward'] = int(r['static_reward'])
                r['uncle_inclusion_reward'] = int(r['uncle_inclusion_reward'])
                r['tx_fees'] = int(r['tx_fees'])
                r['transactions'] = None
                r['uncles'] = None

            return [models.Block(**row) for row in rows]

    async def get_account_mined_blocks(self, address, limit, offset, order):
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = """SELECT * FROM blocks WHERE miner= $1 ORDER BY number {} LIMIT $2 OFFSET $3""".format(order)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address, limit, offset)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['is_sequence_sync']

                r['static_reward'] = int(r['static_reward'])
                r['uncle_inclusion_reward'] = int(r['uncle_inclusion_reward'])
                r['tx_fees'] = int(r['tx_fees'])

            return [models.Block(**row) for row in rows]

    async def get_uncle(self, tag):
        if tag.is_hash():
            query = """SELECT * FROM uncles WHERE hash=$1"""
        elif tag.is_number():
            query = """SELECT * FROM uncles WHERE number=$1"""
        else:
            query = """SELECT * FROM uncles WHERE number=(SELECT max(number) FROM uncles)"""

        async with self.pool.acquire() as conn:
            if tag.is_latest():
                row = await conn.fetchrow(query)
            else:
                row = await conn.fetchrow(query, tag.value)
            if row is None:
                return None
            data = dict(row)
            del data['block_hash']
            data['reward'] = int(data['reward'])
            return models.Uncle(**data)

    async def get_uncles(self, limit, offset, order):
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = """SELECT * FROM uncles ORDER BY number {} LIMIT $1 OFFSET $2""".format(order)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, limit, offset)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['block_hash']
                r['reward'] = int(r['reward'])
            return [models.Uncle(**row) for row in rows]

    async def get_account_mined_uncles(self, address, limit, offset, order):
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = """SELECT * FROM uncles WHERE miner=$1 ORDER BY number {} LIMIT $2 OFFSET $3""".format(order)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address, limit, offset)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['block_hash']
                r['reward'] = int(r['reward'])
            return [models.Uncle(**row) for row in rows]

    async def get_block_uncles(self, tag):
        if tag.is_hash():
            query = """SELECT * FROM uncles WHERE block_hash=$1"""
        elif tag.is_number():
            query = """SELECT * FROM uncles WHERE block_number=$1"""
        else:
            query = """SELECT * FROM uncles WHERE block_number=(SELECT max(number) FROM blocks)"""

        async with self.pool.acquire() as conn:
            if tag.is_latest():
                rows = await conn.fetch(query)
            else:
                rows = await conn.fetch(query, tag.value)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['block_hash']
                r['reward'] = int(r['reward'])
            return [models.Uncle(**r) for r in rows]

    async def get_transaction(self, tx_hash):
        query = """SELECT * FROM transactions WHERE hash=$1"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, tx_hash)
            if row is None:
                return None
            row = dict(row)
            return models.Transaction(**row)

    async def get_receipt(self, tx_hash):
        query = """SELECT * FROM receipts WHERE transaction_hash=$1"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, tx_hash)
            if row is None:
                return None
            row = dict(row)
            row['logs'] = await self.get_logs(row['transaction_hash'])
            return models.Receipt(**row)

    async def get_logs(self, tx_hash):
        query = """SELECT * FROM logs WHERE transaction_hash=$1 ORDER BY log_index"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, tx_hash)
            return [models.Log(**r) for r in rows]

    async def get_accounts_balances(self, addresses):
        query = """SELECT a.address, a.balance FROM accounts a
                    INNER JOIN (SELECT address, max(block_number) bn FROM accounts
                                 WHERE address = any($1::text[]) GROUP BY address) gn
                    ON a.address=gn.address AND a.block_number=gn.bn;"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, addresses)
            return [models.Balance(balance=int(r['balance']), address=r['address']) for r in rows]
