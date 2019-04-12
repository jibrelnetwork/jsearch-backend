import logging
from typing import List

import backoff
import click
import psycopg2
from aiopg.sa import Engine, create_engine
from mode import Service, Worker
from sqlalchemy.dialects.postgresql import insert

from jsearch import settings
from jsearch.common.contracts import ERC20_METHODS_IDS
from jsearch.common.logs import configure
from jsearch.common.tables import assets_transfers_t, transactions_t, assets_summary_t
from jsearch.service_bus import (
    service_bus,
    ROUTE_WALLET_HANDLE_ASSETS_UPDATE,
    ROUTE_WALLET_HANDLE_TOKEN_TRANSFER,
    ROUTE_HANDLE_TRANSACTIONS,
    ROUTE_WALLET_HANDLE_ACCOUNT_UPDATE,
)
from jsearch.syncer.database_queries.assets_summary import insert_or_update_assets_summary
from jsearch.utils import Singleton

logger = logging.getLogger('wallet_worker')


class AssetTransferType:
    ERC20_TRANSFER = 'erc20-transfer'
    ERC20_TRANSFER_FROM = 'erc20-transferFrom'
    ETH_TRANSFER = 'eth-transfer'
    ETH_TRANSFER_INTERNAL = 'eth-transfer-internal'


class DatabaseService(Service, Singleton):
    engine: Engine

    def on_init_dependencies(self) -> List[Service]:
        return [service_bus]

    @backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
    async def on_start(self) -> None:
        self.engine = await create_engine(settings.JSEARCH_MAIN_DB)

    async def on_stop(self) -> None:
        self.engine.close()
        await self.engine.wait_closed()

    async def add_assets_transfer_tx(self, tx_data):
        if tx_data['value'] == '0x0':  # contract call
            return
        transfer = {
            'address': tx_data['from'],
            'type': AssetTransferType.ETH_TRANSFER,
            'from': tx_data['from'],
            'to': tx_data['to'],
            'asset_address': None,
            'value': int(tx_data['value'], 16),
            'decimals': 0,
            'tx_data': tx_data,
            'is_forked': False,
            'block_number': tx_data['block_number'],
            'block_hash': tx_data['block_hash'],
            'ordering': '0',  # FIXME !!!,
            'status': tx_data['receipt_status'],
        }
        async with self.engine.acquire() as connection:
            await connection.execute(assets_transfers_t.insert(), **transfer)
            transfer['address'] = tx_data['to']
            await connection.execute(assets_transfers_t.insert(), **transfer)

    async def add_assets_transfer_tx_internal(self, tx_data, internal_tx_data):
        if tx_data['receipt_status'] == 0 or internal_tx_data['status'] != 'success':
            status = 0
        else:
            status = 1

        if internal_tx_data['value'] == 0:  # no value transfered
            return

        transfer = {
            'address': internal_tx_data['from'],
            'type': AssetTransferType.ETH_TRANSFER_INTERNAL,
            'from': internal_tx_data['from'],
            'to': internal_tx_data['to'],
            'asset_address': None,
            'value': internal_tx_data['value'],
            'decimals': 0,
            'tx_data': tx_data,
            'is_forked': False,
            'block_number': tx_data['block_number'],
            'block_hash': tx_data['block_hash'],
            'ordering': '0',  # FIXME !!!,
            'status': status
        }
        async with self.engine.acquire() as connection:
            await connection.execute(assets_transfers_t.insert(), **transfer)
            transfer['address'] = internal_tx_data['to']
            await connection.execute(assets_transfers_t.insert(), **transfer)

    async def add_assets_transfer_token_transfer(self, transfer_data):
        transfer_from_id = ERC20_METHODS_IDS['transferFrom']
        if transfer_data['token_decimals'] is None:
            logger.warning(
                'No decimals for token transfer',
                extra={
                    'token_address': transfer_data['token_address'],
                    'transaction_hash': transfer_data['transaction_hash'],
                }
            )

            transfer_data['token_decimals'] = 18

        async with self.engine.acquire() as connection:
            result = await connection.execute(transactions_t.select().where(
                transactions_t.c.hash == transfer_data['transaction_hash']))
            tx = await result.fetchone()
            tx_data = dict(tx)
            tx_data.pop('address')
            if tx_data['input'].startswith(transfer_from_id):
                transfer_type = AssetTransferType.ERC20_TRANSFER_FROM
            else:
                transfer_type = AssetTransferType.ERC20_TRANSFER
            transfer = {
                'address': transfer_data['from_address'],
                'type': transfer_type,
                'from': transfer_data['from_address'],
                'to': transfer_data['to_address'],
                'asset_address': transfer_data['token_address'],
                'value': transfer_data['token_value'],
                'decimals': transfer_data['token_decimals'],
                'tx_data': tx_data,
                'is_forked': False,
                'block_number': transfer_data['block_number'],
                'block_hash': transfer_data['block_hash'],
                'ordering': '0',  # FIXME !!!,
                'status': 1  # FIXME!!! real status from stream or DB?
            }

            await connection.execute(assets_transfers_t.insert(), **transfer)
            transfer['address'] = transfer_data['to_address']
            await connection.execute(assets_transfers_t.insert(), **transfer)

    async def add_or_update_asset_summary_balance(self, asset_update):
        upsert = insert_or_update_assets_summary(
            address=asset_update['address'],
            asset_address=asset_update['asset_address'],
            value=asset_update['value'],
            decimals=asset_update['decimals']
        )
        async with self.engine.acquire() as connection:
            await connection.execute(upsert)

    async def add_or_update_asset_summary_transfer(self, asset_transfer):
        summary_data = {
            'address': asset_transfer['address'],
            'asset_address': asset_transfer['token_address'],
        }
        q = insert(assets_summary_t).values(tx_number=1, **summary_data)

        upsert = q.on_conflict_do_update(
            index_elements=['address', 'asset_address'],
            set_=dict(tx_number=assets_summary_t.c.tx_number + 1)
        )

        async with self.engine.acquire() as connection:
            await connection.execute(upsert)


service = DatabaseService()


@service_bus.listen_stream(ROUTE_HANDLE_TRANSACTIONS)
async def handle_new_transaction(tx_data):
    logging.info("[WALLET] Handling new Transaction %s", tx_data['hash'])

    await service.add_assets_transfer_tx(tx_data)
    update_data = {
        'address': tx_data['to'],
        'token_address': '',
    }
    await service.add_or_update_asset_summary_transfer(update_data)
    for itx in tx_data['internal_transactions']:
        if itx['value'] > 0:
            await service.add_assets_transfer_tx_internal(tx_data, itx)


@service_bus.listen_stream(ROUTE_WALLET_HANDLE_ACCOUNT_UPDATE)
async def handle_new_account(account_data):
    logging.info("[WALLET] Handling  Account Update %s", account_data['address'])

    update_data = {
        'address': account_data['address'],
        'asset_address': '',
        'value': account_data['balance'],
        'decimals': 0
    }
    await service.add_or_update_asset_summary_balance(update_data)


@service_bus.listen_stream(ROUTE_WALLET_HANDLE_TOKEN_TRANSFER)
async def handle_token_transfer(transfers):
    for transfer_data in transfers:
        logging.info("[WALLET] Handling new Token Transfer %s block %s %s",
                     transfer_data['address'], transfer_data['block_number'], transfer_data['block_hash'])
        await service.add_assets_transfer_token_transfer(transfer_data)
        await service.add_or_update_asset_summary_transfer(transfer_data)


@service_bus.listen_stream(ROUTE_WALLET_HANDLE_ASSETS_UPDATE)
async def handle_assets_update(updates):
    for update_data in updates:
        logging.info("[WALLET] Handling new Asset Update %s account %s",
                     update_data['asset_address'], update_data['address'])
        await service.add_or_update_asset_summary_balance(update_data)


@click.command()
@click.option('--log-level', default='INFO')
def main(log_level: str) -> None:
    configure(log_level)
    Worker(service, loglevel=log_level).execute_from_commandline()
