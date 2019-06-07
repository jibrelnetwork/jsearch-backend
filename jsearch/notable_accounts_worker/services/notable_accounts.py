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
    listener_service_name = 'jsearch-notable-accounts-worker'
    listener_route = ROUTE_HANDLE_NOTABLE_ACCOUNTS

    def __init__(self, db_dsn: str, update_if_exists: bool, **kwargs: Any) -> None:
        self.database = services.DatabaseService(dsn=db_dsn)
        self.update_if_exists = update_if_exists

        super().__init__(**kwargs)

    def on_init_dependencies(self) -> List[mode.Service]:
        return [self.database, service_bus]

    async def on_start(self) -> None:
        await self.register_listener()

    async def register_listener(self):
        # WTF: Makes a closure with a `NotableAccountsService`s as a context.
        # This is needed to provide `self.database` and `self.update_if_exists`.

        @service_bus.listen_stream(self.listener_route, service_name=self.listener_service_name)
        async def listener(message: Any) -> None:
            logger.info("'NotableAccountsService.listener' received new message")
            await self.handle_notable_accounts(message)

    async def handle_notable_accounts(self, message: Any) -> None:
        logger.info("Handling notable accounts")

        notable_accounts = _load_notable_accounts(message)
        query = _select_insert_query(update_if_exists=self.update_if_exists)

        coros = [_execute_query(self.database.engine, query(account)) for account in notable_accounts]

        await asyncio.gather(*coros)


def _load_notable_accounts(message: Any) -> List[structs.NotableAccount]:
    loaded = list()

    logger.info("Loading notable accounts")

    try:
        for item in message:
            loaded.append(structs.NotableAccount.from_mapping(item))
    except (ValueError, TypeError, KeyError) as e:
        raise InvalidMessageFormat from e

    logger.info("Loaded batch of notable accounts", extra={'count': len(loaded)})

    return loaded


def _select_insert_query(update_if_exists: bool) -> Callable[[structs.NotableAccount], Query]:
    if update_if_exists:
        return database_queries.insert_or_update_notable_account

    return database_queries.insert_or_skip_notable_account


async def _execute_query(engine: Engine, query: Query):
    async with engine.acquire() as conn:
        logger.debug('Executing notable accounts insert query', extra={'query': query})
        await conn.execute(query)
        logger.debug('Executed notable accounts insert query')
