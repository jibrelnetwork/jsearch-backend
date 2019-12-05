import asyncio
import os

import aiopg.sa
from aiohttp import web
from aiohttp.web_app import Application
from aiohttp_swagger import setup_swagger
from psycopg2.extras import DictCursor

from jsearch import settings
from jsearch.api.handlers import contracts
from jsearch.api.handlers import monitoring, blocks, uncles, explorer, tokens, node_proxy, wallets
from jsearch.api.handlers.accounts import (
    get_account,
    get_accounts_balances,
    get_account_internal_txs,
    get_account_pending_txs,
    get_account_transactions,
    get_account_mined_blocks,
    get_account_mined_uncles,
    get_account_token_transfers,
    get_account_token_balance,
    get_account_token_balances_multi,
    get_account_logs,
    get_account_transaction_count,
    get_account_eth_transfers
)
from jsearch.api.middlewares import cors_middleware, prom_middleware
from jsearch.api.node_proxy import NodeProxy
from jsearch.api.storage import Storage
from jsearch.common import logs, stats

swagger_file = os.path.join(os.path.dirname(__file__), 'swagger', 'jsearch-v1.swagger.yaml')
swagger_ui_path = os.path.join(os.path.dirname(__file__), 'swagger', 'ui')


async def on_shutdown(app):
    app['db_pool'].close()
    await app['db_pool'].wait_closed()


def define_routes(app: Application):
    add = app.router.add_route

    add('GET', '/healthcheck', monitoring.healthcheck)
    add('GET', '/metrics', monitoring.metrics)

    add('GET', '/v1/accounts/balances', get_accounts_balances)
    add('GET', '/v1/accounts/{address}', get_account)
    add('GET', '/v1/accounts/{address}/transactions', get_account_transactions, name='accounts_txs')
    add('GET', '/v1/accounts/{address}/internal_transactions', get_account_internal_txs, name='accounts_internal_txs')
    add('GET', '/v1/accounts/{address}/pending_transactions', get_account_pending_txs, name='accounts_pending_txs')
    add('GET', '/v1/accounts/{address}/mined_blocks', get_account_mined_blocks, name='accounts_mined_blocks')
    add('GET', '/v1/accounts/{address}/mined_uncles', get_account_mined_uncles, name='accounts_mined_uncles')
    add('GET', '/v1/accounts/{address}/token_transfers', get_account_token_transfers, name='account_transfers')
    add('GET', '/v1/accounts/{address}/token_balance/{token_address}', get_account_token_balance)
    add('GET', '/v1/accounts/{address}/token_balances', get_account_token_balances_multi)
    add('GET', '/v1/accounts/{address}/logs', get_account_logs, name='accounts_logs')
    add('GET', '/v1/accounts/{address}/transaction_count', get_account_transaction_count)
    add('GET', '/v1/accounts/{address}/eth_transfers', get_account_eth_transfers, name='accounts_eth_transfers')

    add('GET', '/v1/blocks', blocks.get_blocks, name='blocks')
    add('GET', '/v1/blocks/{tag}', blocks.get_block)
    add('GET', '/v1/blocks/{tag}/transactions', blocks.get_block_transactions)
    add('GET', '/v1/blocks/{tag}/uncles', blocks.get_block_uncles)
    add('GET', '/v1/blocks/{tag}/internal_transactions', blocks.get_block_internal_transactions)

    add('GET', '/v1/transactions/{txhash}', explorer.get_transaction)
    add('GET', '/v1/transactions/{txhash}/internal_transactions', explorer.get_internal_transactions)
    add('GET', '/v1/receipts/{txhash}', explorer.get_receipt)

    add('GET', '/v1/uncles', uncles.get_uncles, name='uncles')
    add('GET', '/v1/uncles/{tag}', uncles.get_uncle)

    add('POST', '/v1/verify_contract', contracts.verify_contract)

    add('GET', '/v1/tokens/{address}/transfers', tokens.get_token_transfers, name='token_transfers')
    add('GET', '/v1/tokens/{address}/holders', tokens.get_token_holders, name='token_holders')

    add('GET', '/v1/proxy/gas_price', node_proxy.get_gas_price)
    add('POST', '/v1/proxy/transaction_count', node_proxy.get_transaction_count)
    add('POST', '/v1/proxy/estimate_gas', node_proxy.calculate_estimate_gas)
    add('POST', '/v1/proxy/call_contract', node_proxy.call_contract)
    add('POST', '/v1/proxy/send_raw_transaction', node_proxy.send_raw_transaction)

    add('GET', '/v1/blockchain_tip', wallets.get_blockchain_tip)
    add('GET', '/v1/wallet/assets_summary', wallets.get_assets_summary)
    add('GET', '/v1/wallet/events', wallets.get_wallet_events, name='wallet_events')


def enable_swagger_docs(app: Application) -> None:
    app.router.add_static('/docs', swagger_ui_path)
    setup_swagger(app, swagger_from_file=swagger_file)


async def make_app() -> Application:
    """
    Create and initialize the application instance.
    """
    app = web.Application(middlewares=(prom_middleware, cors_middleware))
    app.on_shutdown.append(on_shutdown)

    app['db_pool'] = await aiopg.sa.create_engine(
        dsn=settings.JSEARCH_MAIN_DB,
        cursor_factory=DictCursor,
    )
    app['storage'] = Storage(app['db_pool'])
    app['node_proxy'] = NodeProxy(settings.ETH_NODE_URL)

    # Configure service routes
    define_routes(app)
    enable_swagger_docs(app)

    stats.setup_api_metrics(app)

    return app


def run_api(port: int, log_level: str, no_json_formatter: bool) -> None:
    loop = asyncio.get_event_loop()
    application = loop.run_until_complete(make_app())

    logs.configure(
        log_level=log_level,
        formatter_class=logs.select_formatter_class(no_json_formatter),
    )

    web.run_app(application, port=port)
