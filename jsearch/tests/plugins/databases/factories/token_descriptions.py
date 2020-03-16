from random import randint

import factory
import pytest

from jsearch.common.tables import token_descriptions_t
from .common import session, Base, generate_address


class TokenDescriptionModel(Base):
    __table__ = token_descriptions_t
    __mapper_args__ = {
        'primary_key': [
            token_descriptions_t.c.block_hash,
            token_descriptions_t.c.address
        ]
    }


class TokenDescriptionsFactory(factory.alchemy.SQLAlchemyModelFactory):
    token = factory.LazyFunction(generate_address)

    total_supply = factory.LazyFunction(lambda: randint(0, 10 ** 18))
    block_number = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)

    is_forked = False

    class Meta:
        model = TokenDescriptionModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'


@pytest.fixture
def token_descriptions_factory():
    yield TokenDescriptionsFactory
    TokenDescriptionsFactory.reset_sequence()
