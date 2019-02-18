from jsearch_service_bus import SyncServiceBusClient, ServiceBus

from jsearch import settings

ROUTE_HANDLE_ERC20_TRANSFERS = 'jsearch.handle_erc20_transfers'
ROUTE_HANDLE_TRANSACTION_LOGS = 'jsearch.handle_transaction_logs'

ROUTE_GET_CONTRACTS = 'jsearch_contracts.get_contracts'


class JsearchSyncServiceBusClient(SyncServiceBusClient):

    def write_logs(self, logs):
        return self.send_to_stream(ROUTE_HANDLE_TRANSACTION_LOGS, value=logs)


class JsearchServiceBus(ServiceBus):

    async def get_contracts(self, addresses):
        return await self.rpc_call(ROUTE_GET_CONTRACTS, value={'addresses': addresses})


service_bus = JsearchServiceBus('jsearch', bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
sync_client = JsearchSyncServiceBusClient('jsearch_backend', bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
