import factory
import pytest

from jsearch.common.tables import wallet_events_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class WalletEventsModel(Base):
    __table__ = wallet_events_t
    __mapper_args__ = {
        'primary_key': [
            wallet_events_t.c.address,
            wallet_events_t.c.tx_hash,
            wallet_events_t.c.block_hash,
            wallet_events_t.c.block_number,
        ]
    }


class WalletEventsFactory(factory.alchemy.SQLAlchemyModelFactory):
    address = factory.LazyFunction(generate_address)
    block_number = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)
    tx_hash = factory.LazyFunction(generate_address)
    event_index = factory.Sequence(lambda n: n % 10)
    type = 'erc20-transfer'
    is_forked = False

    class Meta:
        model = WalletEventsModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'


@pytest.fixture()
def wallet_events_factory():
    return WalletEventsFactory
