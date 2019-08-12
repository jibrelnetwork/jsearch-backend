from random import randint

import factory
import pytest
import time
from eth_utils import keccak, to_normalized_address
from uuid import uuid4

from jsearch.common.tables import uncles_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import Base, session


class UncleModel(Base):
    __table__ = uncles_t


class UncleFactory(factory.alchemy.SQLAlchemyModelFactory):
    hash = factory.LazyFunction(generate_address)
    parent_hash = factory.LazyFunction(generate_address)
    number = factory.Sequence(lambda n: n)

    block_hash = factory.LazyFunction(generate_address)
    block_number = factory.Sequence(lambda n: n)

    difficulty = factory.LazyFunction(lambda: 2800000000000000 + randint(100000000, 10000000000000))
    gas_used = factory.LazyAttribute(lambda self: self.gas_limit - randint(0, self.gas_limit))
    miner = factory.LazyFunction(lambda: to_normalized_address(keccak(text=str(uuid4()))[-20:]))
    timestamp = factory.LazyFunction(time.time)

    # constants or do better latter
    gas_limit = 8000000
    reward = 3000000000000000000
    is_forked = False

    extra_data = ""
    logs_bloom = ""
    mix_hash = ""
    nonce = ""
    sha3_uncles = ""
    size = None
    state_root = ""
    receipts_root = ""
    total_difficulty = None
    transactions_root = ""

    class Meta:
        model = UncleModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'


@pytest.fixture()
def uncle_factory():
    yield UncleFactory
    UncleFactory.reset_sequence()
