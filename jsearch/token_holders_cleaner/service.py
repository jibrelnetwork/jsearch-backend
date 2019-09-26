"""
            select token_address, account_address, block_number
                from token_holders
                where token_address = '0x4dd672e77c795844fe3a464ef8ef0faae617c8fb'
                    and account_address > '0xc72cc5d91a08b36b32b00aa42142ab29237c56c8'

            union all
            select token_address, account_address, block_number
                from token_holders
                where token_address > '0x4dd672e77c795844fe3a464ef8ef0faae617c8fb'
                order by token_address, account_address, block_number limit 100;



            delete from token_holders
                where token_address = '0xa34d29cf8a06e8d05d22454cc33e5fbe7a3d3213'
                    and account_address = '0x7ba732b1bb952155b720250b477ce154e19ad62f'
                    and block_number < (select max(block_number) - 6 from token_holders
                                            where token_address = '0xa34d29cf8a06e8d05d22454cc33e5fbe7a3d3213'
                                            and account_address = '0x7ba732b1bb952155b720250b477ce154e19ad62f'
                                            and is_forked=false);


0x9e9801bace260f58407c15e6e515c45918756e0f', '0x4e7afc9fdad8f1235f43983dafa8bab704664dbd

 0x125791524f4a870ccc1c738271107ceddd8a7d26 | 0x1ac6c687d8d441495aec21adf4ea00f7afb8e71c |      8500037
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0x0e063d3e8d1346cae7ea83912da279ee4332a2eb |      8500009
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0x1ab667300c400e570ea2f4e227d5ef4801b38686 |      8500006
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0x393a0c93d5328fa36ecf79d599a6cc24eade8ceb |      8500011
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0x56eaec1c08e0edf0132f3abafab5a4cd8e39cab5 |      8500003
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0x5bf638bd3a5b34dc8a2d4195a76d717403350db8 |      8500038
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0x6385e5365b4b45335208846bc434e61996e380cf |      8500031
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0x64bc8b2a7ff30dddc80d257f9033ac4f838b34bc |      8500029
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0x7aa4f1e71333c4d199440f792e93c80c14b99e16 |      8500019
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0xc4e03f5519ddbd5699c19cf1b36101940c136bee |      8500000
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0xc6fc14d9cd1ee67f1871ed8c81a8110bbbe98d5a |      8500007
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0xc82a6beef52549305348e0356f6bf32c477bf1a0 |      8500024
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0xd49f9b82253738e7b1d95ce6c309ef88cba362d0 |      8500023
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0xdb1ad30dfb48316c7ceed1107e86dd1ebcb2e4e9 |      8500035

               token_address                |              account_address               | block_number
--------------------------------------------+--------------------------------------------+--------------
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0x56eaec1c08e0edf0132f3abafab5a4cd8e39cab5 |      8500003
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0x5bf638bd3a5b34dc8a2d4195a76d717403350db8 |      8500038
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0x6385e5365b4b45335208846bc434e61996e380cf |      8500031
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0x64bc8b2a7ff30dddc80d257f9033ac4f838b34bc |      8500029
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0x7aa4f1e71333c4d199440f792e93c80c14b99e16 |      8500019
 0x12d7d45a4b9693b312ede375074a48b9b9f2b6ec | 0xc4e03f5519ddbd5699c19cf1b36101940c136bee |      8500000

"""
import asyncio
import logging

import aiopg
import mode
from psycopg2.extras import DictCursor


logger = logging.getLogger(__name__)


class TokenHoldersCleaner(mode.Service):

    def __init__(self, main_db_dsn: str, *args, **kwargs) -> None:
        self.main_db_dsn = main_db_dsn
        self.total = 0
        super().__init__(*args, **kwargs)

    async def on_start(self) -> None:
        await self.connect()

    async def on_stop(self) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        self.conn = await aiopg.connect(self.main_db_dsn, cursor_factory=DictCursor)

    async def disconnect(self) -> None:
        self.conn.close()

    @mode.Service.task
    async def main_loop(self):
        logger.info('Enter main loop')
        last_scanned = {'token_address': '0', 'account_address': '0'}
        while not self.should_stop:
            last_scanned = await self.clean_next_batch(last_scanned)
            if last_scanned is None:
                last_scanned = {'token_address': '0', 'account_address': '0'}
                logger.info('Starting new iteration')
                break
        logger.info('Leaving main loop')

    async def clean_next_batch(self, last_scanned):
        logger.info('Fetching next batch')
        holders = await self.get_next_batch(last_scanned)
        self.total += len(holders)
        logger.info('%s items to process, total %s', len(holders), self.total)

        for holder in holders:
            await self.clean_holder(holder)
        if holders:
            last = holders[-1]
            return {'token_address': last['token_address'], 'account_address': last['account_address']}

    async def get_next_batch(self, last_scanned):
        q = """
            select token_address, account_address, block_number
                from token_holders
                where token_address = %(token_address)s
                    and account_address > %(account_address)s
            union all
            select token_address, account_address, block_number
                from token_holders
                where token_address > %(token_address)s
                order by token_address, account_address, block_number limit 100;
        """
        async with self.conn.cursor() as cur:
            await cur.execute(q, last_scanned)
            rows = await cur.fetchall()
        return rows


    async def clean_holder(self, holder):
        q = """
            delete from token_holders
                where token_address = %(token_address)s
                    and account_address = %(account_address)s
                    and block_number < (select max(block_number) - 6 from token_holders
                                            where token_address = %(token_address)s
                                            and account_address = %(account_address)s
                                            and is_forked=false);
        """
        async with self.conn.cursor() as cur:
            await cur.execute(q, holder)
            logger.debug('Clean for %s: %s', holder, cur.statusmessage)