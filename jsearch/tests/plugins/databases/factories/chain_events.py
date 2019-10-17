import factory
import pytest

from jsearch.common.tables import chain_events_t
from .common import Base, generate_address, session


class ChainEventModel(Base):
    __table__ = chain_events_t
    __mapper_args__ = {
        'primary_key': [
            chain_events_t.c.id
        ]
    }


class ChainEventFactory(factory.alchemy.SQLAlchemyModelFactory):
    id = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)
    block_number = factory.Sequence(lambda n: n)
    drop_length = factory.Sequence(lambda n: n % 6)
    drop_block_hash = factory.LazyFunction(generate_address)
    add_length = factory.Sequence(lambda n: n % 6)
    add_block_hash = factory.LazyFunction(generate_address)
    node_id = factory.Sequence(lambda n: n % 10)

    class Meta:
        model = ChainEventModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'

    @classmethod
    def create_for_block(cls, block, *args, **kwargs):
        cls.create(block_hash=block.hash, block_number=block.number, type='created', *args, **kwargs)


@pytest.fixture()
def chain_events_factory():
    yield ChainEventFactory
    ChainEventFactory.reset_sequence()
