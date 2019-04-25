from typing import List, Any

from mode import Service

from jsearch.post_processing.worker_logs import handle_transaction_logs
from jsearch.post_processing.worker_transfers import handle_new_transfers
from jsearch.service_bus import (
    ROUTE_HANDLE_ERC20_TRANSFERS,
    ROUTE_HANDLE_LAST_BLOCK,
    ROUTE_HANDLE_TRANSACTION_LOGS,
    service_bus,
)
from jsearch.utils import Singleton

ACTION_PROCESS_LOGS = 'logs'
ACTION_PROCESS_TRANSFERS = 'transfers'

ACTION_PROCESS_CHOICES = (
    ACTION_PROCESS_LOGS,
    ACTION_PROCESS_TRANSFERS,
)

WORKER_MAP = {
    ROUTE_HANDLE_LAST_BLOCK: ACTION_PROCESS_TRANSFERS,
    ROUTE_HANDLE_TRANSACTION_LOGS: ACTION_PROCESS_LOGS,
    ROUTE_HANDLE_ERC20_TRANSFERS: ACTION_PROCESS_TRANSFERS,
}

# Workers have to be imported for the `ServiceBus` to register them via
# `@service_bus.listen_stream`.
WORKERS = [handle_new_transfers, handle_transaction_logs]


class PostProcessingService(Singleton, Service):
    def __init__(self, action: str, *args: Any, **kwargs: Any) -> None:
        super(PostProcessingService, self).__init__(*args, **kwargs)

        self.action = action

    def on_init_dependencies(self) -> List[Service]:
        service_bus.streams = {k: v for k, v in service_bus.streams.items() if WORKER_MAP[k] == self.action}

        return [service_bus]

    async def on_start(self) -> None:
        await service_bus.maybe_start()

    async def on_stop(self) -> None:
        await service_bus.stop()
