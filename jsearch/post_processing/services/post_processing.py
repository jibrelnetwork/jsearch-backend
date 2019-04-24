from mode import Service
from typing import List, Any

from jsearch.post_processing.worker_logs import handle_transaction_logs
from jsearch.post_processing.worker_transfers import handle_new_transfers
from jsearch.service_bus import service_bus
from jsearch.utils import Singleton

ACTION_PROCESS_CHOICES = (
    handle_transaction_logs.tag,
    handle_new_transfers.tag,
)


class PostProcessingService(Singleton, Service):
    def __init__(self, action: str, *args: Any, **kwargs: Any) -> None:
        super(PostProcessingService, self).__init__(*args, **kwargs)

        self.action = action

    def on_init_dependencies(self) -> List[Service]:
        service_bus.streams = {k: v for k, v in service_bus.streams.items() if v.tag == self.action}

        return [service_bus]

    async def on_start(self) -> None:
        await service_bus.maybe_start()

    async def on_stop(self) -> None:
        await service_bus.stop()
