import os
import json

import asyncio
import asyncpg
from aiohttp import web
from aiohttp_swagger import setup_swagger


from jsearch.api.storage import Storage
from jsearch.api import handlers


swagger_file = os.path.join(os.path.dirname(__file__), 'swagger', 'jsearch-v1.swagger.yaml')
swagger_ui_path = os.path.join(os.path.dirname(__file__), 'swagger', 'ui')


async def make_app():
    """
    Create and initialize the application instance.
    """
    app = web.Application()
    # Create a database connection pool
    app['db_pool'] = await asyncpg.create_pool(dsn=os.environ.get('DATABASE_URL'))
    app['storage'] = Storage(app['db_pool'])
    app['node_proxy_url'] = os.environ.get('NODE_PROXY_URL')
    # Configure service routes
    app.router.add_route('GET', '/accounts/{address}', handlers.get_account)
    app.router.add_route('GET', '/accounts/{address}/transactions', handlers.get_account_transactions)
    app.router.add_route('GET', '/accounts/{address}/mined_blocks', handlers.get_account_mined_blocks)

    app.router.add_route('GET', '/blocks/{tag}', handlers.get_block)
    app.router.add_route('GET', '/blocks/{tag}/transactions', handlers.get_block_transactions)
    app.router.add_route('GET', '/blocks/{tag}/uncles', handlers.get_block_uncles)

    app.router.add_route('GET', '/transactions/{txhash}', handlers.get_transaction)
    app.router.add_route('GET', '/receipts/{txhash}', handlers.get_receipt)

    app.router.add_route('POST', '/web3', handlers.call_web3_method)

    app.router.add_static('/apidoc', swagger_ui_path)

    setup_swagger(app, swagger_from_file=swagger_file)
    return app


loop = asyncio.get_event_loop()
app = loop.run_until_complete(make_app())


if __name__ == '__main__':
    web.run_app(app)
