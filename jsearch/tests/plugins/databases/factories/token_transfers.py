from random import randint

import factory
import pytest
import time

from jsearch.common.tables import token_transfers_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class TokenTransferModel(Base):
    __table__ = token_transfers_t
    __mapper_args__ = {
        'primary_key': [
            token_transfers_t.c.address,
            token_transfers_t.c.block_hash,
            token_transfers_t.c.transaction_hash,
            token_transfers_t.c.transaction_index,
            token_transfers_t.c.log_index
        ]
    }


class TokenTransferFactory(factory.alchemy.SQLAlchemyModelFactory):
    from_address = factory.LazyFunction(generate_address)
    to_address = factory.LazyFunction(generate_address)
    address = factory.LazyFunction(generate_address)

    transaction_hash = factory.LazyFunction(generate_address)
    transaction_index = factory.LazyFunction(lambda: randint(0, 30))

    log_index = factory.LazyFunction(lambda: randint(0, 30))

    block_number = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)

    token_address = factory.LazyFunction(generate_address)
    token_value = factory.LazyFunction(lambda: randint(0, 10 ** 18))
    token_decimals = factory.LazyFunction(lambda: randint(10, 18))
    token_name = factory.Faker('cryptocurrency_name', locale='en_US')
    token_symbol = factory.Faker('cryptocurrency_code', locale='en_US')

    timestamp = factory.LazyFunction(time.time)

    is_forked = False
    status = factory.LazyFunction(lambda: randint(0, 1))

    class Meta:
        model = TokenTransferModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'

    @classmethod
    def create_for_log(cls, block, tx, log, **kwargs):
        data = factory.build(dict, FACTORY_CLASS=TokenTransferFactory)
        data.update({
            **kwargs,
            **{
                'block_number': block.number,
                'block_hash': block.hash,
                'timestamp': block.timestamp,
                'address': getattr(tx, "from"),
                'from_address': getattr(tx, "from"),
                'to_address': getattr(tx, "to"),
                'transaction_hash': tx.hash,
                'transaction_index': tx.transaction_index,
                'log_index': log.log_index,
            }
        })
        return cls.create_denormalized(**data)

    @classmethod
    def create_denormalized(cls, **kwargs):
        data = factory.build(dict, FACTORY_CLASS=TokenTransferFactory, **kwargs)

        return [
            cls.create(**{**data, 'address': data['from_address']}),
            cls.create(**{**data, 'address': data['to_address']}),
        ]


@pytest.fixture()
def transfer_factory():
    return TokenTransferFactory
