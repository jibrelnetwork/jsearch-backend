import logging
from typing import List, Any, Dict

import mode

from jsearch import settings

from jsearch.common import services
from jsearch.service_bus import service_bus, ROUTE_HANDLE_NOTABLE_ACCOUNTS

logger = logging.getLogger(__name__)
database = services.DatabaseService(dsn=settings.JSEARCH_MAIN_DB)


class NotableAccountsService(mode.Service):
    def __init__(self, update_if_exists: bool, **kwargs: Any) -> None:
        self.update_if_exists = update_if_exists

        super().__init__(**kwargs)


    def on_init_dependencies(self) -> List[mode.Service]:
        return [service_bus, database]

    @service_bus.listen_stream(ROUTE_HANDLE_NOTABLE_ACCOUNTS, service_name='jsearch-notable-accounts-worker')
    async def listener(self, data: List[Dict[str, Any]]) -> None:
        await self.handle_notable_accounts(data)

    async def handle_notable_accounts(self, data: List[Dict[str, Any]]) -> None:
        ...
