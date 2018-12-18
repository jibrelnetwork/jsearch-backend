import os

import asyncio
import asyncpg
from aiohttp import web
from aiohttp_swagger import setup_swagger


from jsearch.api.storage import Storage
from jsearch.common.database import MainDB
from jsearch.api import handlers
from jsearch import settings


swagger_file = os.path.join(os.path.dirname(__file__), 'swagger', 'jsearch-v1.swagger.yaml')
swagger_ui_path = os.path.join(os.path.dirname(__file__), 'swagger', 'ui')


async def on_shutdown(app):
    await app['db_pool'].close()
    await app['main_db'].disconnect()


async def make_app():
    """
    Create and initialize the application instance.
    """
    app = web.Application()
    app.on_shutdown.append(on_shutdown)
    # Create a database connection pool
    app['db_pool'] = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)

    app['storage'] = Storage(app['db_pool'])
    app['main_db'] = MainDB(settings.JSEARCH_MAIN_DB)
    await app['main_db'].connect()
    app['node_proxy_url'] = settings.ETH_NODE_URL
    # Configure service routes
    app.router.add_route('GET', '/accounts/balances', handlers.get_accounts_balances)
    app.router.add_route('GET', '/accounts/{address}', handlers.get_account)
    app.router.add_route('GET', '/accounts/{address}/transactions', handlers.get_account_transactions)
    app.router.add_route('GET', '/accounts/{address}/mined_blocks', handlers.get_account_mined_blocks)
    app.router.add_route('GET', '/accounts/{address}/mined_uncles', handlers.get_account_mined_uncles)
    app.router.add_route('GET', '/accounts/{address}/token_transfers', handlers.get_account_token_transfers)

    app.router.add_route('GET', '/blocks', handlers.get_blocks)
    app.router.add_route('GET', '/blocks/{tag}', handlers.get_block)
    app.router.add_route('GET', '/blocks/{tag}/transactions', handlers.get_block_transactions)
    app.router.add_route('GET', '/blocks/{tag}/uncles', handlers.get_block_uncles)

    app.router.add_route('GET', '/transactions/{txhash}', handlers.get_transaction)
    app.router.add_route('GET', '/receipts/{txhash}', handlers.get_receipt)

    app.router.add_route('GET', '/uncles', handlers.get_uncles)
    app.router.add_route('GET', '/uncles/{tag}', handlers.get_uncle)

    app.router.add_route('POST', '/verify_contract', handlers.verify_contract)

    app.router.add_route('GET', '/tokens/{address}/transfers', handlers.get_token_transfers)

    app.router.add_route('POST', '/_on_new_contracts_added', handlers.on_new_contracts_added)

    app.router.add_static('/apidoc', swagger_ui_path)
    setup_swagger(app, swagger_from_file=swagger_file)
    return app


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(make_app())
    web.run_app(app)
