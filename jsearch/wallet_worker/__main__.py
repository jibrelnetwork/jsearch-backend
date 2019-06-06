import asyncio
import logging

import aiomonitor
import click

from jsearch import settings
from jsearch.common import worker, logs
from jsearch.service_bus import (
    service_bus,
    ROUTE_WALLET_HANDLE_ASSETS_UPDATE,
    ROUTE_WALLET_HANDLE_TOKEN_TRANSFER,
    ROUTE_HANDLE_TRANSACTIONS,
    ROUTE_WALLET_HANDLE_ACCOUNT_UPDATE
)
from jsearch.wallet_worker.api_service import ApiService
from jsearch.wallet_worker.service import DatabaseService

logger = logging.getLogger('wallet_worker')

service = DatabaseService(dsn=settings.JSEARCH_MAIN_DB)


@service_bus.listen_stream(ROUTE_HANDLE_TRANSACTIONS, service_name='jsearch_wallet_worker', task_limit=100)
async def handle_new_transaction(tx_data):
    logger.info(
        "Handling new Transaction",
        extra={
            'tag': 'WALLET',
            'tx_hash': tx_data['hash'],
        }
    )
    await service.add_wallet_event_tx(tx_data)
    update_data = {
        'address': tx_data['to'],
        'token_address': '',
    }
    await service.add_or_update_asset_summary_from_transfer(update_data)
    for itx in tx_data['internal_transactions']:
        await service.add_wallet_event_tx_internal(tx_data, itx)


@service_bus.listen_stream(ROUTE_WALLET_HANDLE_ACCOUNT_UPDATE, service_name='jsearch_wallet_worker', task_limit=100)
async def handle_new_account(account_data):
    logger.info(
        "Handling  Account Update",
        extra={
            'tag': 'WALLET',
            'address': account_data['address'],
        }
    )

    update_data = {
        'address': account_data['address'],
        'asset_address': '',
        'value': account_data['balance'],
        'decimals': 0
    }
    await service.add_or_update_asset_summary_balance(update_data)


@service_bus.listen_stream(ROUTE_WALLET_HANDLE_TOKEN_TRANSFER, service_name='jsearch_wallet_worker', task_limit=100)
async def handle_token_transfer(transfers):
    for transfer_data in transfers:
        logger.info(
            "Handling new Token Transfer",
            extra={
                'tag': 'WALLET',
                'address': transfer_data['address'],
                'block_number': transfer_data['block_number'],
                'block_hash': transfer_data['block_hash'],
            },
        )
        await service.add_wallet_event_token_transfer(transfer_data)
        await service.add_or_update_asset_summary_from_transfer(transfer_data)


@service_bus.listen_stream(ROUTE_WALLET_HANDLE_ASSETS_UPDATE, service_name='jsearch_wallet_worker', task_limit=100)
async def handle_assets_update(updates):
    for update_data in updates:
        logger.info(
            "Handling new Asset Update",
            extra={
                'tag': 'WALLET',
                'address': update_data['address'],
                'asset_address': update_data['asset_address'],
            },
        )

        await service.add_or_update_asset_summary_balance(update_data)


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL)
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
def main(log_level: str, no_json_formatter: bool) -> None:
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))
    loop = asyncio.get_event_loop()
    with aiomonitor.start_monitor(loop=loop):
        worker.Worker(
            service,
            ApiService(),
            loop=loop,
        ).execute_from_commandline()
