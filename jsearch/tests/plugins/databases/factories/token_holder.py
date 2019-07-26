from random import randint

import factory
import pytest

from jsearch.common.tables import token_holders_t
from .common import session, Base, generate_address


class TokenHolderModel(Base):
    __table__ = token_holders_t


class TokenHolderFactory(factory.alchemy.SQLAlchemyModelFactory):
    account_address = factory.LazyFunction(generate_address)
    token_address = factory.LazyFunction(generate_address)

    balance = factory.LazyFunction(lambda: randint(0, 10 ** 18))
    decimals = factory.LazyFunction(lambda: randint(10, 18))
    block_number = factory.Sequence(lambda n: n)

    class Meta:
        model = TokenHolderModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'


@pytest.fixture
def token_holder_factory():
    return TokenHolderFactory
