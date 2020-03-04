from random import randint
from time import time
from typing import Callable, Any, Dict, Optional

import factory
import pytest

from jsearch.common.processing.dex_logs import DexEventType
from jsearch.common.tables import dex_logs_t
from jsearch.common.wallet_events import make_event_index_for_internal_tx
from .common import generate_address, Base, session


class DexLogModel(Base):
    __table__ = dex_logs_t
    __mapper_args__ = {
        'primary_key': [
            dex_logs_t.c.event_index,
        ]
    }

    def to_dict(self):
        return {k: getattr(self, k) for k in self.__table__.columns.keys()}


class DexTokenFactory(factory.DictFactory):
    userAddress = factory.LazyFunction(generate_address)
    assetAddress = factory.LazyFunction(generate_address)
    assetAmount = factory.sequence(lambda n: int(n * 10 ** 3 / randint(1, 99)))


class DexTradeFactory(factory.DictFactory):
    tradeID = factory.sequence(lambda n: n)


class DexTradePlacedFactory(DexTradeFactory):
    tradeCreator = factory.LazyFunction(generate_address)
    tradedAmount = factory.Sequence(lambda n: n * randint(1000, 10000))
    tradeID = factory.sequence(lambda n: n)


class DexOrderFactory(factory.DictFactory):
    orderID = factory.Sequence(lambda n: n)


class DexOrderPlacedFactory(DexOrderFactory):
    orderType = factory.Sequence(lambda n: n % 2)
    orderCreator = factory.LazyFunction(generate_address)
    tradedAsset = factory.LazyFunction(generate_address)
    tradedAmount = factory.Sequence(lambda n: n * randint(1000, 10000))
    fiatAsset = factory.LazyFunction(generate_address)
    assetPrice = factory.Sequence(lambda n: n % 70)
    expirationTimestamp = factory.Sequence(lambda n: int(time()) + n * 10 ** 3)


class DexLogFactory(factory.alchemy.SQLAlchemyModelFactory):
    block_number = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)
    timestamp = factory.LazyFunction(lambda: int(time()))
    tx_hash = factory.LazyFunction(generate_address)
    event_type = factory.Sequence(lambda n: DexEventType.ALL[n % len(DexEventType.ALL)])
    event_index = factory.Sequence(lambda n: make_event_index_for_internal_tx(n, n, n))
    is_forked = False

    @factory.lazy_attribute
    def event_data(self):
        return create_dex_event_dict(event_type=self.event_type)['event_args']

    class Meta:
        model = DexLogModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'


FACTORIES = {
    DexEventType.ORDER_PLACED: DexOrderPlacedFactory,
    DexEventType.ORDER_EXPIRED: DexOrderFactory,
    DexEventType.ORDER_CANCELLED: DexOrderFactory,
    DexEventType.ORDER_COMPLETED: DexOrderFactory,
    DexEventType.ORDER_ACTIVATED: DexOrderFactory,

    DexEventType.TRADE_PLACED: DexTradePlacedFactory,
    DexEventType.TRADE_COMPLETED: DexTradeFactory,
    DexEventType.TRADE_CANCELLED: DexTradeFactory,

    DexEventType.TOKEN_BLOCKED: DexTokenFactory,
    DexEventType.TOKEN_UNBLOCKED: DexTokenFactory,
}


def create_dex_event_args(event_type, **kwargs) -> Dict[str, Any]:
    assert event_type in DexEventType.ALL

    factory_cls = FACTORIES[event_type]
    return factory.build(dict, FACTORY_CLASS=factory_cls, **kwargs)


def create_dex_event_dict(
        *,
        event_type: str = DexEventType.ORDER_PLACED,
        event_args: Optional[Dict[str, Any]] = None, **kwargs: Any):
    from jsearch import settings
    event_args = create_dex_event_args(event_type, **event_args or {})
    kwargs.update({
        'address': settings.DEX_CONTRACT,
        'event_type': event_type,
        'event_args': event_args,
    })
    return kwargs


@pytest.fixture
def dex_log_factory() -> Callable[..., Dict[str, Any]]:
    def _factory(
            *,
            event_type: str = DexEventType.ORDER_PLACED,
            event_args: Optional[Dict[str, Any]] = None,
            **kwargs
    ) -> Dict[str, Any]:
        event_args = create_dex_event_args(event_type=event_type, **event_args)
        data = factory.build(
            dict,
            FACTORY_CLASS=DexLogFactory,
            event_type=event_type,
            event_data=event_args,
            **kwargs
        )
        DexLogFactory.create(**data)
        return data

    return _factory
