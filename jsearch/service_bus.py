from jsearch_service_bus import ServiceBus

from jsearch import settings


class JsearchBackendServiceBus(ServiceBus):
    async def get_contracts(self, *args, **kwargs):
        return await self.rpc_call('jsearch_contracts.get_contracts', *args, **kwargs)


service_bus = JsearchBackendServiceBus('jsearch_backend', bootstrap_servers=settings.KAFKA)
