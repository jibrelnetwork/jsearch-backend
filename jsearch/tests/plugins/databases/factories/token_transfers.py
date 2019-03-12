import time
from random import randint

import factory
import pytest

from jsearch.common.tables import token_transfers_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class TokenTransferModel(Base):
    __table__ = token_transfers_t
    __mapper_args__ = {
        'primary_key': [
            token_transfers_t.c.address,
            token_transfers_t.c.transaction_hash,
            token_transfers_t.c.transaction_index
        ]
    }


class TokenTransferFactory(factory.alchemy.SQLAlchemyModelFactory):
    from_address = factory.LazyFunction(generate_address)
    to_address = factory.LazyFunction(generate_address)
    address = factory.LazyFunction(generate_address)

    transaction_hash = factory.LazyFunction(generate_address)
    transaction_index = factory.LazyFunction(lambda: randint(0, 30))

    block_number = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)

    token_address = factory.LazyFunction(generate_address)
    token_value = factory.LazyFunction(lambda: randint(0, 10 ** 18))
    token_decimals = factory.LazyFunction(lambda: randint(10, 18))
    token_name = factory.Faker('cryptocurrency_name', locale='en_US')
    token_symbol = factory.Faker('cryptocurrency_code', locale='en_US')

    timestamp = factory.LazyFunction(time.time)

    is_forked = False

    class Meta:
        model = TokenTransferModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'


@pytest.fixture()
def transfer_factory():
    return TokenTransferFactory
