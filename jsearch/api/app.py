import asyncio
import os

import asyncpg
import sentry_sdk
from aiohttp import web
from aiohttp_swagger import setup_swagger

from jsearch import settings
from jsearch.api.handlers import contracts
from jsearch.api.handlers import monitoring, accounts, blocks, uncles, explorer, tokens, node_proxy, wallets
from jsearch.api.middlewares import cors_middleware
from jsearch.api.node_proxy import NodeProxy
from jsearch.api.storage import Storage
from jsearch.common import logs, stats

swagger_file = os.path.join(os.path.dirname(__file__), 'swagger', 'jsearch-v1.swagger.yaml')
swagger_ui_path = os.path.join(os.path.dirname(__file__), 'swagger', 'ui')

sentry_sdk.init(settings.RAVEN_DSN)


async def on_shutdown(app):
    await app['db_pool'].close()


async def make_app():
    """
    Create and initialize the application instance.
    """
    app = web.Application(middlewares=(cors_middleware,))
    app.on_shutdown.append(on_shutdown)
    # Create a database connection pool
    app['db_pool'] = await asyncpg.create_pool(dsn=settings.JSEARCH_MAIN_DB)
    app['storage'] = Storage(app['db_pool'])
    app['node_proxy'] = NodeProxy(settings.ETH_NODE_URL)

    # Configure service routes
    app.router.add_route('GET', '/healthcheck', monitoring.healthcheck)
    app.router.add_route('GET', '/metrics', monitoring.metrics)

    app.router.add_route('GET', '/v1/accounts/balances', accounts.get_accounts_balances)
    app.router.add_route('GET', '/v1/accounts/{address}', accounts.get_account)
    app.router.add_route(
        'GET', '/v1/accounts/{address}/transactions', accounts.get_account_transactions, name='accounts_txs'
    )
    app.router.add_route(
        'GET',
        '/v1/accounts/{address}/internal_transactions',
        accounts.get_account_internal_transactions,
        name='accounts_internal_txs'
    )
    app.router.add_route(
        'GET',
        '/v1/accounts/{address}/pending_transactions',
        accounts.get_account_pending_transactions,
        name='accounts_pending_txs'
    )
    app.router.add_route('GET', '/v1/accounts/{address}/mined_blocks', accounts.get_account_mined_blocks)
    app.router.add_route('GET', '/v1/accounts/{address}/mined_uncles', accounts.get_account_mined_uncles)
    app.router.add_route('GET', '/v1/accounts/{address}/token_transfers', accounts.get_account_token_transfers)
    app.router.add_route(
        'GET', '/v1/accounts/{address}/token_balance/{token_address}', accounts.get_account_token_balance
    )
    app.router.add_route('GET', '/v1/accounts/{address}/token_balances', accounts.get_account_token_balances_multi)
    app.router.add_route('GET', '/v1/accounts/{address}/logs', accounts.get_account_logs, name='accounts_logs')
    app.router.add_route('GET', '/v1/accounts/{address}/transaction_count', accounts.get_account_transaction_count)

    app.router.add_route('GET', '/v1/blocks', blocks.get_blocks, name='blocks')
    app.router.add_route('GET', '/v1/blocks/{tag}', blocks.get_block)
    app.router.add_route('GET', '/v1/blocks/{tag}/transactions', blocks.get_block_transactions)
    app.router.add_route('GET', '/v1/blocks/{tag}/uncles', blocks.get_block_uncles)
    app.router.add_route('GET', '/v1/blocks/{tag}/internal_transactions', blocks.get_block_internal_transactions)

    app.router.add_route('GET', '/v1/transactions/{txhash}', explorer.get_transaction)
    app.router.add_route('GET', '/v1/transactions/{txhash}/internal_transactions', explorer.get_internal_transactions)
    app.router.add_route('GET', '/v1/receipts/{txhash}', explorer.get_receipt)

    app.router.add_route('GET', '/v1/uncles', uncles.get_uncles, name='uncles')
    app.router.add_route('GET', '/v1/uncles/{tag}', uncles.get_uncle)

    app.router.add_route('POST', '/v1/verify_contract', contracts.verify_contract)

    app.router.add_route('GET', '/v1/tokens/{address}/transfers', tokens.get_token_transfers)
    app.router.add_route('GET', '/v1/tokens/{address}/holders', tokens.get_token_holders)

    app.router.add_route('GET', '/v1/proxy/gas_price', node_proxy.get_gas_price)
    app.router.add_route('POST', '/v1/proxy/transaction_count', node_proxy.get_transaction_count)
    app.router.add_route('POST', '/v1/proxy/estimate_gas', node_proxy.calculate_estimate_gas)
    app.router.add_route('POST', '/v1/proxy/call_contract', node_proxy.call_contract)
    app.router.add_route('POST', '/v1/proxy/send_raw_transaction', node_proxy.send_raw_transaction)

    app.router.add_route('GET', '/v1/blockchain_tip', wallets.get_blockchain_tip)
    app.router.add_route('GET', '/v1/wallet/assets_summary', wallets.get_assets_summary)
    app.router.add_route('GET', '/v1/wallet/transfers', wallets.get_wallet_transfers)
    app.router.add_route('GET', '/v1/wallet/transactions', wallets.get_wallet_transactions)
    app.router.add_route('GET', '/v1/wallet/events', wallets.get_wallet_events, name='wallet_events')

    app.router.add_static('/docs', swagger_ui_path)
    setup_swagger(app, swagger_from_file=swagger_file)

    stats.setup_api_metrics()
    logs.configure(
        log_level=settings.LOG_LEVEL,
        formatter_class=logs.select_formatter_class(settings.NO_JSON_FORMATTER),
    )

    return app


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    application = loop.run_until_complete(make_app())
    web.run_app(application)
