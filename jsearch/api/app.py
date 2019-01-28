import asyncio
import os

import asyncpg
import sentry_sdk
from aiohttp import web
from aiohttp_swagger import setup_swagger

from jsearch import settings
from jsearch.api import handlers
from jsearch.api.storage import Storage

swagger_file = os.path.join(os.path.dirname(__file__), 'swagger', 'jsearch-v1.swagger.yaml')
swagger_ui_path = os.path.join(os.path.dirname(__file__), 'swagger', 'ui')

sentry_sdk.init(settings.RAVEN_DSN)


async def on_shutdown(app):
    await app['db_pool'].close()


async def make_app():
    """
    Create and initialize the application instance.
    """
    app = web.Application()
    app.on_shutdown.append(on_shutdown)
    # Create a database connection pool
    app['db_pool'] = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)
    app['storage'] = Storage(app['db_pool'])

    # Configure service routes
    app.router.add_route('GET', '/v1/accounts/balances', handlers.get_accounts_balances)
    app.router.add_route('GET', '/v1/accounts/{address}', handlers.get_account)
    app.router.add_route('GET', '/v1/accounts/{address}/transactions', handlers.get_account_transactions)
    app.router.add_route('GET', '/v1/accounts/{address}/mined_blocks', handlers.get_account_mined_blocks)
    app.router.add_route('GET', '/v1/accounts/{address}/mined_uncles', handlers.get_account_mined_uncles)
    app.router.add_route('GET', '/v1/accounts/{address}/token_transfers', handlers.get_account_token_transfers)
    app.router.add_route('GET', '/v1/accounts/{address}/token_balance/{token_address}', handlers.get_account_token_balance)

    app.router.add_route('GET', '/v1/blocks', handlers.get_blocks)
    app.router.add_route('GET', '/v1/blocks/{tag}', handlers.get_block)
    app.router.add_route('GET', '/v1/blocks/{tag}/transactions', handlers.get_block_transactions)
    app.router.add_route('GET', '/v1/blocks/{tag}/uncles', handlers.get_block_uncles)

    app.router.add_route('GET', '/v1/transactions/{txhash}', handlers.get_transaction)
    app.router.add_route('GET', '/v1/receipts/{txhash}', handlers.get_receipt)

    app.router.add_route('GET', '/v1/uncles', handlers.get_uncles)
    app.router.add_route('GET', '/v1/uncles/{tag}', handlers.get_uncle)

    app.router.add_route('POST', '/v1/verify_contract', handlers.verify_contract)

    app.router.add_route('GET', '/v1/tokens/{address}/transfers', handlers.get_token_transfers)
    app.router.add_route('GET', '/v1/tokens/{address}/holders', handlers.get_token_holders)

    app.router.add_route('POST', '/_on_new_contracts_added', handlers.on_new_contracts_added)

    app.router.add_static('/docs', swagger_ui_path)
    setup_swagger(app, swagger_from_file=swagger_file)
    return app


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(make_app())
    web.run_app(app)
