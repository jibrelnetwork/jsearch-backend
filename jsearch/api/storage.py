from typing import List

from jsearch.api import models
from jsearch.api.models.all import TokenTransfer

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
            query = """
                SELECT block_number, block_hash, address, nonce, balance FROM accounts_state
                WHERE address=$1
                    AND block_number<=(SELECT number FROM blocks WHERE hash=$2)
                ORDER BY block_number DESC LIMIT 1;
            """
        elif tag.is_number():
            query = """
                SELECT block_number, block_hash, address, nonce, balance FROM accounts_state
                WHERE address=$1 AND block_number<=$2
                ORDER BY block_number DESC LIMIT 1;
            """
        else:
            query = """
                SELECT "block_number", "block_hash", "address", "nonce", "balance" FROM accounts_state
                WHERE address=$1 ORDER BY block_number DESC LIMIT 1;
            """
        async with self.pool.acquire() as conn:
            if tag.is_latest():
                state_row = await conn.fetchrow(query, address)
            else:
                state_row = await conn.fetchrow(query, address, tag.value)

            if state_row is None:
                return None

            state_row = dict(state_row)
            state_row['balance'] = int(state_row['balance'])

            query = """
                SELECT address, code, code_hash FROM accounts_base
                WHERE address=$1 LIMIT 1;
            """
            base_row = dict(await conn.fetchrow(query, address))

            row = {}
            row.update(state_row)
            row.update(base_row)

            return models.Account(**row)

    async def get_account_transactions(self, address, limit, offset):
        fields = models.Transaction.select_fields()

        query = f"""
            SELECT {fields} FROM transactions
            WHERE "to"=$1 OR "from"=$1
            ORDER BY block_number, transaction_index LIMIT $2 OFFSET $3
        """

        limit = min(limit, MAX_ACCOUNT_TRANSACTIONS_LIMIT)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address.lower(), limit, offset)
            rows = [dict(r) for r in rows]
            return [models.Transaction(**r) for r in rows]

    async def get_block_transactions(self, tag):
        fields = models.Transaction.select_fields()

        if tag.is_hash():
            query = f"SELECT {fields} FROM transactions WHERE block_hash=$1 AND is_forked=false " \
                        f"ORDER BY transaction_index;"
        elif tag.is_number():
            query = f"SELECT {fields} FROM transactions WHERE block_number=$1 AND is_forked=false " \
                        f"ORDER BY transaction_index;"
        else:
            query = f"""
                SELECT {fields} FROM transactions
                WHERE block_number=(SELECT max(number) FROM blocks) AND is_forked=false ORDER BY transaction_index;
        """

        async with self.pool.acquire() as conn:
            if tag.is_latest():
                rows = await conn.fetch(query)
            else:
                rows = await conn.fetch(query, tag.value)
            rows = [dict(r) for r in rows]
            return [models.Transaction(**r) for r in rows]

    async def get_block(self, tag):

        if tag.is_hash():
            query = "SELECT * FROM blocks WHERE hash=$1 AND is_forked=false"
        elif tag.is_number():
            query = "SELECT * FROM blocks WHERE number=$1 AND is_forked=false"
        else:
            query = "SELECT * FROM blocks WHERE number=(SELECT max(number) FROM blocks) AND is_forked=false"

        tx_query = "SELECT hash FROM transactions WHERE block_hash=$1 ORDER BY transaction_index"
        async with self.pool.acquire() as conn:
            if tag.is_latest():
                row = await conn.fetchrow(query)
            else:
                row = await conn.fetchrow(query, tag.value)
            if row is None:
                return None
            data = dict(row)
            del data['is_sequence_sync']
            del data['is_forked']

            data['static_reward'] = int(data['static_reward'])
            data['uncle_inclusion_reward'] = int(data['uncle_inclusion_reward'])
            data['tx_fees'] = int(data['tx_fees'])

            txs = await conn.fetch(tx_query, data['hash'])
            data['transactions'] = [tx['hash'] for tx in txs]
            data['uncles'] = None
            return models.Block(**data)

    async def get_blocks(self, limit, offset, order):
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = f"""SELECT * FROM blocks WHERE is_forked=false ORDER BY number {order} LIMIT $1 OFFSET $2"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, limit, offset)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['is_sequence_sync']
                del r['is_forked']

                r['static_reward'] = int(r['static_reward'])
                r['uncle_inclusion_reward'] = int(r['uncle_inclusion_reward'])
                r['tx_fees'] = int(r['tx_fees'])
                r['transactions'] = None
                r['uncles'] = None

            return [models.Block(**row) for row in rows]

    async def get_account_mined_blocks(self, address, limit, offset, order):
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = f"""SELECT * FROM blocks WHERE miner= $1 ORDER BY number {order} LIMIT $2 OFFSET $3"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address, limit, offset)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['is_sequence_sync']
                del r['is_forked']
                r['transactions'] = None
                r['uncles'] = None
                r['static_reward'] = int(r['static_reward'])
                r['uncle_inclusion_reward'] = int(r['uncle_inclusion_reward'])
                r['tx_fees'] = int(r['tx_fees'])

            return [models.Block(**row) for row in rows]

    async def get_uncle(self, tag):
        if tag.is_hash():
            query = "SELECT * FROM uncles WHERE hash=$1"
        elif tag.is_number():
            query = "SELECT * FROM uncles WHERE number=$1"
        else:
            query = "SELECT * FROM uncles WHERE number=(SELECT max(number) FROM uncles)"

        async with self.pool.acquire() as conn:
            if tag.is_latest():
                row = await conn.fetchrow(query)
            else:
                row = await conn.fetchrow(query, tag.value)
            if row is None:
                return None
            data = dict(row)
            del data['block_hash']
            del data['is_forked']
            data['reward'] = int(data['reward'])
            return models.Uncle(**data)

    async def get_uncles(self, limit, offset, order):
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = f"""SELECT * FROM uncles ORDER BY number {order} LIMIT $1 OFFSET $2"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, limit, offset)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['block_hash']
                del r['is_forked']
                r['reward'] = int(r['reward'])
            return [models.Uncle(**row) for row in rows]

    async def get_account_mined_uncles(self, address, limit, offset, order):
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = f"""SELECT * FROM uncles WHERE miner=$1 ORDER BY number {order} LIMIT $2 OFFSET $3"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address, limit, offset)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['block_hash']
                r['reward'] = int(r['reward'])
            return [models.Uncle(**row) for row in rows]

    async def get_block_uncles(self, tag):
        if tag.is_hash():
            query = "SELECT * FROM uncles WHERE block_hash=$1"
        elif tag.is_number():
            query = "SELECT * FROM uncles WHERE block_number=$1"
        else:
            query = "SELECT * FROM uncles WHERE block_number=(SELECT max(number) FROM blocks)"

        async with self.pool.acquire() as conn:
            if tag.is_latest():
                rows = await conn.fetch(query)
            else:
                rows = await conn.fetch(query, tag.value)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['block_hash']
                del r['is_forked']
                r['reward'] = int(r['reward'])
            return [models.Uncle(**r) for r in rows]

    async def get_transaction(self, tx_hash):
        fields = models.Transaction.select_fields()
        query = f"SELECT {fields} FROM transactions WHERE hash=$1"

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, tx_hash)
            if row is None:
                return None
            row = dict(row)
            return models.Transaction(**row)

    async def get_receipt(self, tx_hash):
        query = "SELECT * FROM receipts WHERE transaction_hash=$1"
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, tx_hash)
            if row is None:
                return None
            row = dict(row)
            del row['is_forked']
            row['logs'] = await self.get_logs(row['transaction_hash'])
            return models.Receipt(**row)

    async def get_logs(self, tx_hash):
        fields = models.Log.select_fields()
        query = f"SELECT {fields} FROM logs WHERE transaction_hash=$1 ORDER BY log_index"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, tx_hash)
            return [models.Log(**r) for r in rows]

    async def get_accounts_balances(self, addresses):
        query = """SELECT a.address, a.balance FROM accounts_state a
                    INNER JOIN (SELECT address, max(block_number) bn FROM accounts_state
                                 WHERE address = any($1::text[]) GROUP BY address) gn
                    ON a.address=gn.address AND a.block_number=gn.bn;"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, addresses)
            addr_map = {r['address']: r for r in rows}
            return [models.Balance(balance=int(addr_map[a]['balance']), address=addr_map[a]['address'])
                    for a in addresses if a in addr_map]

    async def _fetch_token_transfers(self, query: str, address: str, limit: int, offset: int) \
            -> List[TokenTransfer]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address, limit, offset)
            transfers: List[models.TokenTransfer] = []
            for row in rows:
                row = dict(row)
                del row['transaction_index']
                del row['log_index']
                del row['block_number']
                del row['block_hash']
                transfers.append(models.TokenTransfer(**row))
            return transfers

    async def get_tokens_transfers(self, address: str, limit: int, offset: int, order: str) \
            -> List[models.TokenTransfer]:
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        offset *= 2
        limit *= 2
        query = f"""
            SELECT transaction_hash,
                    transaction_index,
                    log_index,
                    block_number,
                    block_hash,
                    timestamp,
                    from_address,
                    to_address,
                    token_address,
                    token_value,
                    token_decimals,
                    token_name,
                    token_symbol
            FROM token_transfers
            WHERE token_address = $1 AND is_forked = false
            ORDER BY block_number {order}, transaction_index {order}, log_index {order} LIMIT $2 OFFSET $3;
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address, limit, offset)
            transfers: List[models.TokenTransfer] = []
            distinct_set = set()
            for row in rows:
                row = dict(row)
                distinct_key = tuple(row.values())
                if distinct_key in distinct_set:
                    continue
                distinct_set.add(distinct_key)
                del row['transaction_index']
                del row['log_index']
                del row['block_number']
                del row['block_hash']
                transfers.append(models.TokenTransfer(**row))
            return transfers

    async def get_account_tokens_transfers(self, address, limit, offset, order):
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = f"""
            SELECT transaction_hash,
                    transaction_index,
                    log_index,
                    block_number,
                    block_hash,
                    timestamp,
                    from_address,
                    to_address,
                    token_address,
                    token_value,
                    token_decimals,
                    token_name,
                    token_symbol
            FROM token_transfers
            WHERE address = $1 AND is_forked = false
            ORDER BY block_number {order}, transaction_index {order}, log_index {order} LIMIT $2 OFFSET $3;
        """
        return await self._fetch_token_transfers(query, address, limit, offset)

    async def get_contact_creation_code(self, address: str) -> str:
        query = """
        SELECT input FROM transactions t
          INNER JOIN receipts r
           ON t.hash = r.transaction_hash
           AND r.contract_address = $1
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, address)
        return row['input']

    async def get_tokens_holders(self, address: str, limit: int, offset: int, order: str) \
            -> List[models.TokenHolder]:
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = f"""
        SELECT account_address, token_address, balance, decimals
        FROM token_holders
        WHERE token_address=$1
        ORDER BY balance {order} LIMIT $2 OFFSET $3;
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address, limit, offset)
            return [models.TokenHolder(**r) for r in rows]

    async def get_account_token_balance(self, account_address: str, token_address: str) \
            -> List[models.TokenHolder]:
        query = """
        SELECT account_address, token_address, balance, decimals
        FROM token_holders
        WHERE account_address=$1 AND token_address=$2
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, account_address, token_address)
            return models.TokenHolder(**row) if row else None

    async def get_blockchain_tip_status(self, last_known_block_hash: str):
        """
        {
          “data“: {
            “blockchainTip“: {
              “blockHash“: string,
              “blockNumber“: string
            },
            “forkData“: {
              “isInFork“: true/false,
              “lastUnchangedBlock“: number
            }
          },
          “status“: {..}
        }

         id                  | bigint                | not null default nextval('chain_splits_id_seq'::regclass)
         common_block_number | bigint                | not null
         common_block_hash   | character varying(70) | not null
         drop_length         | bigint                | not null
         drop_block_hash     | character varying(70) | not null
         add_length          | bigint                | not null
         add_block_hash      | character varying(70) | not null
         node_id             | character varying(70) | not null

        :return:
        """
        split_query = "SELECT common_block_number, drop_block_hash FROM chain_splits WHERE drop_block_hash=$1"
        block_query = "SELECT number FROM blocks WHERE hash=$1"

        async with self.pool.acquire() as conn:
            split = await conn.fetchrow(split_query, last_known_block_hash)
        async with self.pool.acquire() as conn:
            block = await conn.fetchrow(block_query, last_known_block_hash)

        if split is None:
            result = {
                'blockchainTip': {
                  'blockHash': last_known_block_hash,
                  'blockNumber': block['number']
                },
                'forkData': {
                  'isInFork': False,
                  'lastUnchangedBlock': block['number']
                }
            }
        else:
            result = {
                'blockchainTip': {
                  'blockHash': last_known_block_hash,
                  'blockNumber': block['number']
                },
                'forkData': {
                  'isInFork': True,
                  'lastUnchangedBlock': split['common_block_number']
                }
            }
        return result
