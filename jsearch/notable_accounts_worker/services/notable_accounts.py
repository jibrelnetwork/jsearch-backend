import collections
import logging
from typing import List, Any, Generator

import mode

from jsearch.common import services
from jsearch.notable_accounts_worker import structs
from jsearch.service_bus import service_bus, ROUTE_HANDLE_NOTABLE_ACCOUNTS

logger = logging.getLogger(__name__)


class InvalidMessageFormat(BaseException):
    pass


class NotableAccountsService(mode.Service):
    def __init__(self, db_dsn: str, update_if_exists: bool, **kwargs: Any) -> None:
        self.database = services.DatabaseService(dsn=db_dsn)
        self.update_if_exists = update_if_exists

        super().__init__(**kwargs)


    def on_init_dependencies(self) -> List[mode.Service]:
        return [service_bus, self.database]

    @service_bus.listen_stream(ROUTE_HANDLE_NOTABLE_ACCOUNTS, service_name='jsearch-notable-accounts-worker')
    async def listener(self, message: Any) -> None:
        await self.handle_notable_accounts(message)

    async def handle_notable_accounts(self, message: Any) -> None:
        notable_accounts = _load_notable_accounts(message)


def _load_notable_accounts(message: Any) -> List[structs.NotableAccount]:
    loaded = list()

    try:
        for item in message:
            loaded.append(structs.NotableAccount.from_mapping(item))
    except (ValueError, TypeError, KeyError) as e:
        raise InvalidMessageFormat from e

    return loaded
