r"""
Communication scheme for token transfer reorganization.

 --------                                    -------------------
| raw_db |                                  | jsearch_contracts |
 --------                                    -------------------
     |                                            /'\
     |  reorg record                               |
     |                                             | get contracts
    \./                   jsearch.reorganization   |
 --------  reorg event  -----------------------------   balance     -----------
| syncer | - - - - - > | handle_block_reorganization | < - - - ->  | geth node |
 --------  block hash   -----------------------------   request     -----------
                                 /'\   |
                                  |    |
           get token and accounts |   \./ update balances
                            ----------------
                           |     main db    |
                            ----------------

"""

import asyncio
import logging
from collections import defaultdict
from functools import partial
from typing import List

import backoff
import click
import psycopg2
from aiopg.sa import Engine, create_engine
from sqlalchemy.dialects.postgresql import insert
from mode import Service, Worker

from jsearch import settings
from jsearch.common.logs import configure
from jsearch.common.tables import assets_transfers_t, transactions_t, assets_summary_t
from jsearch.common.processing.erc20_balances import fetch_erc20_token_balance
from jsearch.multiprocessing import executor
from jsearch.post_processing.metrics import Metrics
from jsearch.service_bus import (
    service_bus,
    WALLET_HANDLE_NEW_TRANSACTION,
    WALLET_HANDLE_NEW_ACCOUNT,
    WALLET_HANDLE_TOKEN_TRANSFER,
    WALLET_HANDLE_ASSETS_UPDATE,
)
from jsearch.syncer.database_queries.token_holders import update_token_holder_balance_q
from jsearch.syncer.database_queries.token_transfers import get_token_address_and_accounts_for_block_q
from jsearch.utils import Singleton


logger = logging.getLogger('wallet_worker')


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

    async def add_assets_transafer_tx(self, tx_data):
        transfer = {
            'address': tx_data['from'],
            'type': 'eth-transfer',
            'from':  tx_data['from'],
            'to':  tx_data['to'],
            'asset_address':  None,
            'amount':  tx_data['value'],
            'tx_data': tx_data,
            'is_forked':  False,
            'block_number': tx_data['block_number'],
            'block_hash': tx_data['block_hash'],
            'ordering': '0',  # FIXME !!!
        }
        async with self.engine.acquire() as connection:
            await connection.execute(assets_transfers_t.insert(), **transfer)
            transfer['address'] = tx_data['to']
            await connection.execute(assets_transfers_t.insert(), **transfer)

    async def add_assets_transfer_token_transfer(self, transfer_data):
        async with self.engine.acquire() as connection:
            result = await connection.execute(transactions_t.select().where(
                transactions_t.c.hash == transfer_data['transaction_hash']))
            tx = await result.fetchone()
            tx_data = dict(tx)
            tx_data.pop('address')
            amount = transfer_data['token_value'] / (10 ** transfer_data['token_decimals'])
            print('AAAAAAA', amount, transfer_data['token_value'], transfer_data['token_decimals'])
            transfer = {
                'address': transfer_data['from_address'],
                'type': 'erc20-transfer',
                'from':  transfer_data['from_address'],
                'to':  transfer_data['to_address'],
                'asset_address':  transfer_data['token_address'],
                'amount':  str(amount),
                'tx_data': tx_data,
                'is_forked':  False,
                'block_number': transfer_data['block_number'],
                'block_hash': transfer_data['block_hash'],
                'ordering': '0',  # FIXME !!!
            }

            await connection.execute(assets_transfers_t.insert(), **transfer)
            transfer['address'] = transfer_data['to_address']
            await connection.execute(assets_transfers_t.insert(), **transfer)

    async def add_or_update_asset_summary_balance(self, asset_update):
        summary_data = {
            'address': asset_update['address'],
            'asset_address': asset_update['asset_address'],
            'balance': hex(asset_update['balance']),
        }
        q = insert(assets_summary_t).values(tx_number=1, **summary_data)

        upsert = q.on_conflict_do_update(
            index_elements=['address', 'asset_address'],
            set_=dict(balance=summary_data['balance'])
        )

        async with self.engine.acquire() as connection:
            await connection.execute(upsert)

    async def add_or_update_asset_summary_transfer(self, asset_transfer):
        summary_data = {
            'address': asset_transfer['address'],
            'asset_address': asset_transfer['asset_address'],
        }
        q = insert(assets_summary_t).values(tx_number=1, **summary_data)

        upsert = q.on_conflict_do_update(
            index_elements=['address', 'asset_address'],
            set_=dict(tx_number=assets_summary_t.c.tx_number + 1)
        )

        async with self.engine.acquire() as connection:
            await connection.execute(upsert)



service = DatabaseService()


@service_bus.listen_stream(WALLET_HANDLE_NEW_TRANSACTION)
async def handle_new_transaction(tx_data):
    logging.info("[WALLET] Handling new Transaction %s", tx_data['hash'])
    await service.add_assets_transafer_tx(tx_data)


@service_bus.listen_stream(WALLET_HANDLE_NEW_ACCOUNT)
async def handle_new_account(block_hash, block_number, account_data):
    logging.info("[WALLET] Handling new Account %s block %s %s", account_data['address'], block_number, block_hash)


@service_bus.listen_stream(WALLET_HANDLE_TOKEN_TRANSFER)
async def handle_token_transfer(transfers):
    for transfer_data in transfers:
        logging.info("[WALLET] Handling new Token Transfer %s block %s %s",
                     transfer_data['address'], transfer_data['block_number'], transfer_data['block_hash'])
        await service.add_assets_transfer_token_transfer(transfer_data)
        await service.add_or_update_asset_summary_transfer(transfer_data)


@service_bus.listen_stream(WALLET_HANDLE_ASSETS_UPDATE)
async def handle_assets_update(updates):
    for update_data in updates:
        logging.info("[WALLET] Handling new Asset Update %s account %s",
                     update_data['asset_address'], update_data['address'])



@click.command()
@click.option('--log-level', default='INFO')
def main(log_level: str) -> None:
    configure(log_level)
    Worker(service, loglevel=log_level).execute_from_commandline()