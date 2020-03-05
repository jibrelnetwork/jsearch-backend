from dataclasses import dataclass, field
from functools import partial
from random import randint
from typing import Callable, Dict, Any, Optional, List

import pytest

from jsearch.common.processing.dex_logs import DexEventType
from jsearch.common.wallet_events import make_event_index_for_log
from jsearch.tests.plugins.databases.factories.common import generate_address


@pytest.fixture()
def token_address():
    return generate_address()


@pytest.fixture()
def order_creator():
    return generate_address()


def prepare_kwargs(common_keys=('block_number', 'timestamp', 'event_index'), **kwargs) -> Dict[str, Any]:
    common = {key: value for key, value in kwargs.items() if key in common_keys}
    event_args = {key: value for key, value in kwargs.items() if key not in common_keys}
    return {
        **common,
        'event_args': {
            **event_args,
        }
    }


@dataclass
class DexLinkedEventsFactory:
    create_event: Callable[..., Dict[str, Any]]

    # cache
    history: List[Dict[str, Any]] = field(init=False, default_factory=list)

    _order: Optional[Dict[str, Any]] = field(init=False, default=None)
    _trade: Optional[Dict[str, Any]] = field(init=False, default=None)

    @property
    def order_id(self) -> str:
        order = getattr(self, '_order', None)
        if not order:
            order = self.add_order()

        return order['orderID']

    @property
    def trade_id(self):
        trade = getattr(self, '_trade', None)
        if not trade:
            trade = self.add_trade()
        return trade['tradeID']

    def event_factory(self, **kwargs: Any) -> Dict[str, Any]:
        event = self.create_event(**kwargs)
        payload = event.pop('event_data')
        last = {
            **event,
            **payload,
        }
        self.history.append(last)
        return last

    def add_order(self, **kwargs: Any) -> Dict[str, Any]:
        kwargs = prepare_kwargs(**kwargs)
        self._order = self.event_factory(**kwargs)
        return self._order

    def add_order_status(self, event_type: str, **kwargs: Any) -> Dict[str, Any]:
        assert event_type in DexEventType.ORDERS and not event_type == DexEventType.ORDER_PLACED

        kwargs = prepare_kwargs(orderID=self.order_id, **kwargs)
        return self.event_factory(event_type=event_type, **kwargs)

    def add_trade(self, **kwargs: Any) -> Dict[str, Any]:
        kwargs = prepare_kwargs(orderID=self.order_id, **kwargs)
        self._trade = self.event_factory(event_type=DexEventType.TRADE_PLACED, **kwargs)
        return self._trade

    def add_trade_status(self, event_type: str, **kwargs: Any) -> Dict[str, Any]:
        assert event_type in DexEventType.TRADE and not event_type == DexEventType.TRADE_PLACED
        kwargs = prepare_kwargs(tradeID=self.trade_id, **kwargs)
        return self.event_factory(event_type=event_type, **kwargs)


@pytest.fixture()
def dex_linked_events_factory(dex_log_factory) -> DexLinkedEventsFactory:
    return DexLinkedEventsFactory(create_event=dex_log_factory)


@pytest.fixture()
def random_events(
        token_address: str,
        dex_linked_events_factory: DexLinkedEventsFactory
) -> Callable[[int], List[Dict[str, Any]]]:
    def _factory(limit: int, token: Optional[str] = None) -> List[Dict[str, Any]]:
        token = token or token_address

        kwargs = {
            'block_number': 1,
            'timestamp': 1 * 1000,
            'event_index': make_event_index_for_log(0, 0, 0)
        }

        dex_linked_events_factory.add_order(tradedAsset=token, **kwargs)
        dex_linked_events_factory.add_trade(tradedAsset=token, **kwargs)

        while len(dex_linked_events_factory.history) < limit:
            seed = randint(0, 3)

            n = len(dex_linked_events_factory.history)
            kwargs['event_index'] = make_event_index_for_log(n, n, n)
            # each 3 records algorithm changes block for dex event
            if not (len(dex_linked_events_factory.history) - 1) % 3:
                kwargs['block_number'] += 1
                kwargs['timestamp'] += 1 * 1000

            add_order = partial(dex_linked_events_factory.add_order, tradedAsset=token)
            add_order_status = partial(
                dex_linked_events_factory.add_order_status,
                event_type=[
                    DexEventType.ORDER_EXPIRED,
                    DexEventType.ORDER_COMPLETED,
                    DexEventType.ORDER_CANCELLED,
                    DexEventType.ORDER_ACTIVATED,
                ][randint(0, 3)]
            )
            add_trade = dex_linked_events_factory.add_trade
            add_trade_status = partial(
                dex_linked_events_factory.add_trade_status,
                event_type=[
                    DexEventType.TRADE_CANCELLED,
                    DexEventType.TRADE_COMPLETED,
                ][randint(0, 1)]
            )

            action = [
                add_order,
                add_order_status,
                add_trade,
                add_trade_status
            ][seed]

            action(**kwargs)

        return dex_linked_events_factory.history

    return _factory
