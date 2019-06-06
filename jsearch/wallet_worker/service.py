import logging
from typing import List

from mode import Service
from sqlalchemy.dialects.postgresql import insert

from jsearch.common import services
from jsearch.common.tables import transactions_t, assets_summary_t, wallet_events_t
from jsearch.common.wallet_events import (
    event_from_internal_tx,
    event_from_token_transfer,
    event_from_tx
)
from jsearch.service_bus import service_bus
from jsearch.syncer.database_queries.assets_summary import insert_or_update_assets_summary
from jsearch.utils import Singleton
from jsearch.wallet_worker.typing import (
    AssetTransfer,
    AssetUpdate,
    Event,
    InternalTransaction,
    TokenTransfer,
    Transaction
)

logger = logging.getLogger('wallet_worker')


class DatabaseService(services.DatabaseService, Singleton):
    def on_init_dependencies(self) -> List[Service]:
        return [service_bus]

    async def _insert_event(self, event_data_from: Event, event_data_to: Event) -> None:
        q = wallet_events_t.insert()
        async with self.engine.acquire() as connection:
            async with connection.begin():
                await connection.execute(q, **event_data_from)
                await connection.execute(q, **event_data_to)

    async def add_wallet_event_tx(self, tx_data):
        event_data_from = event_from_tx(tx_data['from'], tx_data)
        event_data_to = event_from_tx(tx_data['to'], tx_data)
        if event_data_to is None:
            return
        await self._insert_event(event_data_from, event_data_to)

    async def add_wallet_event_tx_internal(self, tx_data: Transaction, internal_tx_data: InternalTransaction) -> None:
        event_data_from = event_from_internal_tx(internal_tx_data['from'], internal_tx_data, tx_data)
        if event_data_from is not None:
            event_data_to = event_from_internal_tx(internal_tx_data['to'], internal_tx_data, tx_data)
            await self._insert_event(event_data_from, event_data_to)

    async def add_wallet_event_token_transfer(self, transfer_data: TokenTransfer) -> None:
        async with self.engine.acquire() as connection:
            result = await connection.execute(transactions_t.select().where(
                transactions_t.c.hash == transfer_data['transaction_hash'])
            )
            tx = await result.fetchone()
            tx_data = dict(tx)
            tx_data.pop('address')

            event_data_from = event_from_token_transfer(transfer_data['from_address'], transfer_data, tx_data)
            event_data_to = event_from_token_transfer(transfer_data['to_address'], transfer_data, tx_data)
            q = wallet_events_t.insert()

            async with connection.begin():
                await connection.execute(q.values(**event_data_from))
                await connection.execute(q.values(**event_data_to))

    async def add_or_update_asset_summary_from_transfer(self, asset_transfer: AssetTransfer) -> None:
        summary_data = {
            'address': asset_transfer['address'],
            'asset_address': asset_transfer['token_address'],
        }
        q = insert(assets_summary_t).values(tx_number=1, **summary_data)

        upsert = q.on_conflict_do_update(
            index_elements=['address', 'asset_address'],
            set_=dict(tx_number=assets_summary_t.c.tx_number + 1)
        )

        async with self.engine.acquire() as connection:
            await connection.execute(upsert)

    async def add_or_update_asset_summary_balance(self, asset_update: AssetUpdate) -> None:
        upsert = insert_or_update_assets_summary(
            address=asset_update['address'],
            asset_address=asset_update['asset_address'],
            value=asset_update['value'],
            decimals=asset_update['decimals']
        )
        async with self.engine.acquire() as connection:
            await connection.execute(upsert)
