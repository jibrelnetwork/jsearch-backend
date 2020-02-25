import time
from random import randint
from uuid import uuid4

import factory
import pytest
from eth_utils import keccak, to_normalized_address

from jsearch.common.tables import blocks_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import Base, session


class BlockModel(Base):
    __table__ = blocks_t


class BlockFactory(factory.alchemy.SQLAlchemyModelFactory):
    hash = factory.LazyFunction(generate_address)
    parent_hash = factory.LazyFunction(generate_address)
    number = factory.Sequence(lambda n: n)
    difficulty = factory.LazyFunction(lambda: 2800000000000000 + randint(100000000, 10000000000000))
    gas_used = factory.LazyAttribute(lambda self: self.gas_limit - randint(0, self.gas_limit))
    miner = factory.LazyFunction(lambda: to_normalized_address(keccak(text=str(uuid4()))[-20:]))
    tx_fees = factory.LazyFunction(lambda: randint(50703830640000000, 120499392905312000))
    timestamp = factory.LazyFunction(lambda: int(time.time()))

    # constants or do better latter
    gas_limit = 8000000
    static_reward = 3000000000000000000
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
    is_sequence_sync = False
    uncle_inclusion_reward = 0

    class Meta:
        model = BlockModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'

    @classmethod
    def create_with_event(cls, chain_events_factory, **kwargs) -> BlockModel:
        block = cls.create(**kwargs)
        chain_events_factory.create_block(block)

        return block


@pytest.fixture()
def block_factory():
    yield BlockFactory
    BlockFactory.reset_sequence()


@pytest.fixture()
def block_dict_factory():
    def dict_factory(**kwargs):
        return factory.build(dict, FACTORY_CLASS=BlockFactory, **kwargs)

    yield dict_factory
