import logging

from jsearch_service_bus import SyncServiceBusClient, ServiceBus

from jsearch import settings
from jsearch.typing import Contracts

logger = logging.getLogger(__name__)

SERVICE_JSEARCH = 'jsearch'
SERVICE_CONTRACT = 'jsearch_contracts'

ROUTE_HANDLE_ERC20_TRANSFERS = f'{SERVICE_JSEARCH}.erc20_transfers'
ROUTE_HANDLE_TRANSACTION_LOGS = f'{SERVICE_JSEARCH}.transaction_logs'
ROUTE_HANDLE_REORGANIZATION_EVENTS = f'{SERVICE_JSEARCH}.reorganization'
ROUTE_HANDLE_LAST_BLOCK = f'{SERVICE_JSEARCH}.last_block'

ROUTE_GET_CONTRACTS = f'{SERVICE_CONTRACT}.get_contracts'

ROUTE_HANDLE_TRANSACTIONS = f'{SERVICE_JSEARCH}.transactions'
ROUTE_WALLET_HANDLE_TOKEN_TRANSFER = f'{SERVICE_JSEARCH}.token_transfers'
ROUTE_WALLET_HANDLE_ASSETS_UPDATE = f'{SERVICE_JSEARCH}.asset_updates'
ROUTE_WALLET_HANDLE_ACCOUNT_UPDATE = f'{SERVICE_JSEARCH}.account_update'


class JsearchSyncServiceBusClient(SyncServiceBusClient):

    def write_logs(self, logs):
        return self.send_to_stream(ROUTE_HANDLE_TRANSACTION_LOGS, value=logs)

    def get_contracts(self, addresses, fields=None) -> Contracts:
        return self.rpc_call(ROUTE_GET_CONTRACTS, value={'addresses': addresses, 'fields': fields})

    def write_tx(self, tx):
        return self.send_to_stream(ROUTE_HANDLE_TRANSACTIONS, value=tx)

    def write_account(self, account):
        return self.send_to_stream(ROUTE_WALLET_HANDLE_ACCOUNT_UPDATE, value=account)

    def write_transfers(self, transfers):
        return self.send_to_stream(ROUTE_WALLET_HANDLE_TOKEN_TRANSFER, value=transfers)

    def write_assets_updates(self, updates):
        return self.send_to_stream(ROUTE_WALLET_HANDLE_ASSETS_UPDATE, value=updates)


class JsearchServiceBus(ServiceBus):
    async def emit_last_block_event(self, number):
        return await self.send_to_stream(
            route=ROUTE_HANDLE_LAST_BLOCK,
            value={
                'number': number
            }
        )

    async def emit_reorganization_event(self, block_hash, block_number, reinserted):
        return await self.send_to_stream(
            route=ROUTE_HANDLE_REORGANIZATION_EVENTS,
            value={
                'block_hash': block_hash,
                'block_number': block_number,
                'reinserted': reinserted
            }
        )

    async def send_transfers(self, value):
        return await self.send_to_stream(ROUTE_HANDLE_ERC20_TRANSFERS, value)

    async def get_contracts(self, addresses, fields=None) -> Contracts:
        return await self.rpc_call(ROUTE_GET_CONTRACTS, value={'addresses': addresses, 'fields': fields})


service_bus = JsearchServiceBus('jsearch', bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
sync_client = JsearchSyncServiceBusClient('jsearch', bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
