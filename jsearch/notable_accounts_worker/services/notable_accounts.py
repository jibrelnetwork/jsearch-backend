import asyncio
import logging
from typing import List, Any, Callable

import mode
from aiopg.sa import Engine
from sqlalchemy.orm import Query

from jsearch.common import services
from jsearch.notable_accounts_worker import structs, database_queries
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
        query = _select_insert_query(update_if_exists=self.update_if_exists)

        coros = [_execute_query(self.database.engine, query(account)) for account in notable_accounts]

        await asyncio.gather(*coros)


def _load_notable_accounts(message: Any) -> List[structs.NotableAccount]:
    loaded = list()

    try:
        for item in message:
            loaded.append(structs.NotableAccount.from_mapping(item))
    except (ValueError, TypeError, KeyError) as e:
        raise InvalidMessageFormat from e

    return loaded


def _select_insert_query(update_if_exists: bool) -> Callable[[structs.NotableAccount], Query]:
    if update_if_exists:
        return database_queries.insert_or_update_notable_account

    return database_queries.insert_or_skip_notable_account


async def _execute_query(engine: Engine, query: Query):
    async with engine.acquire() as conn:
        await conn.execute(query)
