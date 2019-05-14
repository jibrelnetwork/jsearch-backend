import asyncio
import logging
import os

import aiomonitor
import backoff
import click
import psycopg2
from aiopg.sa import Engine, create_engine
from mode import Service
from sqlalchemy.dialects.postgresql import insert
from typing import List

from jsearch import settings
from jsearch.common import worker
from jsearch.common.contracts import ERC20_METHODS_IDS, NULL_ADDRESS
from jsearch.common.logs import configure
from jsearch.common.tables import transactions_t, assets_summary_t, wallet_events_t
from jsearch.service_bus import (
    service_bus,
    ROUTE_WALLET_HANDLE_ASSETS_UPDATE,
    ROUTE_WALLET_HANDLE_TOKEN_TRANSFER,
    ROUTE_HANDLE_TRANSACTIONS,
    ROUTE_WALLET_HANDLE_ACCOUNT_UPDATE,
)
from jsearch.syncer.database_queries.assets_summary import insert_or_update_assets_summary
from jsearch.utils import Singleton
from jsearch.wallet_worker.api_service import ApiService

logger = logging.getLogger('wallet_worker')


class WalletEventType:
    ERC20_TRANSFER = 'erc20-transfer'
    ETH_TRANSFER = 'eth-transfer'
    CONTRACT_CALL = 'contract-call'
    TX_CANCELLATION = 'tx-cancellation'


CANCELLATION_ADDRESS = '0x000000000000000000000063616e63656c6c6564'
TOKEN_DECIMALS_DEFAULT = 18


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

    async def insert_event(self, event_data_from, event_data_to):
        q = wallet_events_t.insert()
        async with self.engine.acquire() as connection:
            async with connection.begin():
                await connection.execute(q, **event_data_from)
                await connection.execute(q, **event_data_to)

    async def add_wallet_event_tx_internal(self, tx_data, internal_tx_data):
        event_data_from = event_from_internal_tx(internal_tx_data['from'], internal_tx_data, tx_data)
        if event_data_from is None:
            return
        event_data_to = event_from_internal_tx(internal_tx_data['to'], internal_tx_data, tx_data)
        await self.insert_event(event_data_from, event_data_to)

    async def add_wallet_event_token_transfer(self, transfer_data):
        async with self.engine.acquire() as connection:
            result = await connection.execute(transactions_t.select().where(
                transactions_t.c.hash == transfer_data['transaction_hash']))
            tx = await result.fetchone()
            tx_data = dict(tx)
            tx_data.pop('address')
            event_data_from = event_from_token_transfer(transfer_data['from_address'], transfer_data, tx_data)
            event_data_to = event_from_token_transfer(transfer_data['to_address'], transfer_data, tx_data)
            q = wallet_events_t.insert()

            async with connection.begin():
                await connection.execute(q.values(**event_data_from))
                await connection.execute(q.values(**event_data_to))

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

    async def add_wallet_event_tx(self, tx_data):
        event_data_from = event_from_tx(tx_data['from'], tx_data)
        event_data_to = event_from_tx(tx_data['to'], tx_data)
        if event_data_to is None:
            return
        await self.insert_event(event_data_from, event_data_to)


service = DatabaseService()


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
    await service.add_or_update_asset_summary_transfer(update_data)
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
        await service.add_or_update_asset_summary_transfer(transfer_data)


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


def get_event_type(tx_data):
    if int(tx_data['value'], 16) != 0:
        return WalletEventType.ETH_TRANSFER

    if tx_data['to'] == NULL_ADDRESS:
        return WalletEventType.CONTRACT_CALL

    if tx_data['to_contract'] is True:
        method_id = tx_data['input'][:10]
        if method_id in (ERC20_METHODS_IDS['transferFrom'], ERC20_METHODS_IDS['transfer']):
            return WalletEventType.CONTRACT_CALL
        else:
            return None
    else:
        if tx_data['to'] == CANCELLATION_ADDRESS:
            return WalletEventType.TX_CANCELLATION
    return None


def event_from_tx(address, tx_data):
    """
    Make wallet event object from TX data

    :param  address: from address of to address of Transaction - explicitly
    :param tx_data: full TX data object

    :return: event data object
    """
    event_type = get_event_type(tx_data)
    if event_type is None:
        return None
    event_data = {
        'is_forked': False,
        'address': address,
        'type': event_type,
        'block_number': tx_data['block_number'],
        'block_hash': tx_data['block_hash'],
        'tx_hash': tx_data['hash'],
        'event_index': tx_data['transaction_index'] + tx_data['block_number'] * 1000,
        'tx_data': tx_data,
        'event_data': {'sender': tx_data['from'],
                       'recipient': tx_data['to'],
                       'amount': str(int(tx_data['value'], 16)),
                       'status': tx_data['receipt_status']}
    }
    return event_data


def event_from_token_transfer(address, transfer_data, tx_data):
    """
    Make wallet event object from Transfer and TX data

    :param  address: from address or to address of Transrer - explicitly
    :param tx_data: full TX data object
    :param transfer_data: full Token Transfer data object

    :return: event data object
    """
    event_type = WalletEventType.ERC20_TRANSFER
    decimals = transfer_data['token_decimals'] or TOKEN_DECIMALS_DEFAULT
    amount = str(transfer_data['token_value'] / 10 ** decimals)
    event_data = {
        'is_forked': False,
        'address': address,
        'type': event_type,
        'block_number': tx_data['block_number'],
        'block_hash': tx_data['block_hash'],
        'tx_hash': tx_data['hash'],
        'event_index': tx_data['transaction_index'] + tx_data['block_number'] * 1000,
        'tx_data': tx_data,
        'event_data': {'sender': transfer_data['from_address'],
                       'recipient': transfer_data['to_address'],
                       'amount': amount,
                       'asset': transfer_data['token_address'],
                       'status': transfer_data['status']}
    }
    return event_data


def event_from_internal_tx(address, internal_tx_data, tx_data):
    """
    Make wallet event object from internal TX data and Root TX data

    :param  address: from address or to address of Transaction - explicitly
    :param internal_tx_data: internal TX data object
    :param tx_data: full TX data object

    :return: event data object
    """
    if internal_tx_data['value'] == 0:
        return None
    event_type = WalletEventType.ETH_TRANSFER
    if tx_data['receipt_status'] == 0 or internal_tx_data['status'] != 'success':
        event_status = 0
    else:
        event_status = 1
    event_data = {
        'is_forked': False,
        'address': address,
        'type': event_type,
        'block_number': tx_data['block_number'],
        'block_hash': tx_data['block_hash'],
        'tx_hash': tx_data['hash'],
        'event_index': tx_data['transaction_index'] + tx_data['block_number'] * 1000,
        'tx_data': tx_data,
        'event_data': {'sender': internal_tx_data['from'],
                       'recipient': internal_tx_data['to'],
                       'amount': str(internal_tx_data['value']),
                       'status': event_status}
    }
    return event_data


@click.command()
@click.option('--log-level', default=os.getenv('LOG_LEVEL', 'INFO'))
def main(log_level: str) -> None:
    configure(log_level)
    loop = asyncio.get_event_loop()
    with aiomonitor.start_monitor(loop=loop):
        worker.Worker(
            service,
            ApiService(),
            loop=loop,
        ).execute_from_commandline()
