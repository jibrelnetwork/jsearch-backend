from jsearch_service_bus import SyncServiceBusClient, ServiceBus

from jsearch import settings
from jsearch.typing import Contracts

SERVICE_JSEARCH = 'jsearch'
SERVICE_CONTRACT = 'jsearch_contracts'

WORKER_HANDLE_ERC20_TRANSFERS = 'erc20_transfers'
WORKER_HANDLE_TRANSACTION_LOGS = 'transaction_logs'
WORKER_HANDLE_REORGANIZATION_EVENT = 'reorganization'
WORKER_UPDATE_TOKEN_HOLDER_BALANCE = 'update_token_holder_balance'

ROUTE_HANDLE_ERC20_TRANSFERS = f'{SERVICE_JSEARCH}.{WORKER_HANDLE_ERC20_TRANSFERS}'
ROUTE_HANDLE_TRANSACTION_LOGS = f'{SERVICE_JSEARCH}.{WORKER_HANDLE_TRANSACTION_LOGS}'
ROUTE_HANDLE_UPDATE_TOKEN_HOLDER_BALANCE = f'{SERVICE_JSEARCH}.{WORKER_UPDATE_TOKEN_HOLDER_BALANCE}'
ROUTE_HANDLE_REORGANIZATION_EVENTS = f'{SERVICE_JSEARCH}.{WORKER_HANDLE_REORGANIZATION_EVENT}'

ROUTE_GET_CONTRACTS = f'{SERVICE_CONTRACT}.get_contracts'


class JsearchSyncServiceBusClient(SyncServiceBusClient):

    def write_logs(self, logs):
        return self.send_to_stream(ROUTE_HANDLE_TRANSACTION_LOGS, value=logs)

    def emit_reorganization_event(self, block_hash, block_number, reinserted):
        return self.send_to_stream(
            route=ROUTE_HANDLE_REORGANIZATION_EVENTS,
            value={
                'block_hash': block_hash,
                'block_number': block_number,
                'reinserted': reinserted
            }
        )


class JsearchServiceBus(ServiceBus):
    async def emit_update_token_balance_holder_event(self, token_address: str, account_address: str, reason: str):
        return await self.send_to_stream(
            route=ROUTE_HANDLE_UPDATE_TOKEN_HOLDER_BALANCE,
            value={
                'token_address': token_address,
                'account_address': account_address,
                'reason': reason
            }
        )

    async def send_transfers(self, value):
        return await self.send_to_stream(ROUTE_HANDLE_ERC20_TRANSFERS, value)

    async def get_contracts(self, addresses, fields=None) -> Contracts:
        return await self.rpc_call(ROUTE_GET_CONTRACTS, value={'addresses': addresses, 'fields': fields})


service_bus = JsearchServiceBus('jsearch', bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
sync_client = JsearchSyncServiceBusClient('jsearch_backend', bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
