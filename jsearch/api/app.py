import os
import json

import asyncio
import asyncpg
from aiohttp import web

from jsearch.api.storage import Storage
from jsearch.api import handlers



async def make_app():
    """
    Create and initialize the application instance.
    """
    app = web.Application()
    # Create a database connection pool
    app['db_pool'] = await asyncpg.create_pool(dsn=os.environ.get('DATABASE_URL'))
    app['storage'] = Storage(app['db_pool'])
    # Configure service routes
    app.router.add_route('GET', '/accounts/{address}/', handlers.get_account)
    app.router.add_route('GET', '/accounts/{address}/txs/', handlers.get_account_transactions)

    app.router.add_route('GET', '/blocks/{hashOrNumber}', handlers.get_block)
    app.router.add_route('GET', '/blocks/{hashOrNumber}/txs/', handlers.get_block_transactions)
    app.router.add_route('GET', '/blocks/{hashOrNumber}/uncles/', handlers.get_block_uncles)

    app.router.add_route('GET', '/transactions/{txhash}/', handlers.get_transaction)
    app.router.add_route('GET', '/receipts/{txhash}/', handlers.get_receipt)
    
    app.router.add_route('POST', '/web3/', handlers.call_web3_method)

    return app


loop = asyncio.get_event_loop()
app = loop.run_until_complete(make_app())


if __name__ == '__main__':
    web.run_app(app)
