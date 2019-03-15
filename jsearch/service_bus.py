from jsearch_service_bus import SyncServiceBusClient, ServiceBus

from jsearch import settings
from jsearch.typing import Contracts

SERVICE_JSEARCH = 'jsearch'
SERVICE_CONTRACT = 'jsearch_contracts'

WORKER_HANDLE_ERC20_TRANSFERS = 'erc20_transfers'
WORKER_HANDLE_TRANSACTION_LOGS = 'transaction_logs'
WORKER_HANDLE_REORGANIZATION_EVENT = 'reorganizations'

WALLET_HANDLE_NEW_TRANSACTION = 'WALLET_HANDLE_NEW_TRANSACTION'
WALLET_HANDLE_NEW_ACCOUNT = 'WALLET_HANDLE_NEW_ACCOUNT'
WALLET_HANDLE_TOKEN_TRANSFER = 'WALLET_HANDLE_TOKEN_TRANSFER'
WALLET_HANDLE_ASSETS_UPDATE = 'WALLET_HANDLE_ASSETS_UPDATE'

ROUTE_HANDLE_ERC20_TRANSFERS = f'{SERVICE_JSEARCH}.{WORKER_HANDLE_ERC20_TRANSFERS}'
ROUTE_HANDLE_TRANSACTION_LOGS = f'{SERVICE_JSEARCH}.{WORKER_HANDLE_TRANSACTION_LOGS}'
ROUTE_HANDLE_REORGANIZATION_EVENTS = f'{SERVICE_JSEARCH}.{WORKER_HANDLE_REORGANIZATION_EVENT}'
ROUTE_HANDLE_TRANSACTIONS = f'{SERVICE_JSEARCH}.{WALLET_HANDLE_NEW_TRANSACTION}'
ROUTE_WALLET_HANDLE_TOKEN_TRANSFER = f'{SERVICE_JSEARCH}.{WALLET_HANDLE_TOKEN_TRANSFER}'
ROUTE_WALLET_HANDLE_ASSETS_UPDATE = f'{SERVICE_JSEARCH}.{WALLET_HANDLE_ASSETS_UPDATE}'


ROUTE_GET_CONTRACTS = f'{SERVICE_CONTRACT}.get_contracts'


class JsearchSyncServiceBusClient(SyncServiceBusClient):

    def write_logs(self, logs):
        return self.send_to_stream(ROUTE_HANDLE_TRANSACTION_LOGS, value=logs)

    def write_tx(self, tx):
        return self.send_to_stream(ROUTE_HANDLE_TRANSACTIONS, value=tx)

    def write_transfers(self, transfers):
        return self.send_to_stream(ROUTE_WALLET_HANDLE_TOKEN_TRANSFER, value=transfers)

    def write_assets_updates(self, updates):
        return self.send_to_stream(ROUTE_WALLET_HANDLE_ASSETS_UPDATE, value=updates)


class JsearchServiceBus(ServiceBus):
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
sync_client = JsearchSyncServiceBusClient('jsearch_backend', bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
