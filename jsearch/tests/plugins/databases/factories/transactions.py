import time
from functools import partial

import factory
import pytest

from jsearch.common.tables import transactions_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class TransactionModel(Base):
    __table__ = transactions_t
    __mapper_args__ = {
        'primary_key': [
            transactions_t.c.hash,
            transactions_t.c.block_hash,
            transactions_t.c.address,
            transactions_t.c.transaction_index,
        ]
    }

    def to_dict(self):
        return {k: getattr(self, k) for k in self.__table__.columns.keys()}


class TransactionFactory(factory.alchemy.SQLAlchemyModelFactory):
    from_ = factory.LazyFunction(generate_address)
    to = factory.LazyFunction(generate_address)
    address = factory.LazyFunction(generate_address)

    hash = factory.LazyFunction(generate_address)
    transaction_index = factory.Sequence(lambda n: n % 200)

    block_number = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)
    timestamp = factory.LazyFunction(time.time)

    gas = '0xabc'
    gas_price = '0x123'
    input = '0x00'
    nonce = factory.Sequence(lambda n: n)
    r = '0xaa'
    s = '0xbb'
    v = '0xcc'
    value = factory.Sequence(lambda n: hex(n))
    contract_call_description = {}

    status = 1
    is_forked = False

    class Meta:
        model = TransactionModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'
        rename = {'from_': 'from'}

    @classmethod
    def create_for_block(cls, block, as_dict=False, **kwargs):
        get_block_attr = block.get if isinstance(block, dict) else partial(getattr, block)
        block_data = {
            'block_number': get_block_attr('number'),
            'block_hash': get_block_attr('hash'),
            'timestamp': get_block_attr('timestamp'),
        }

        data = factory.build(dict, FACTORY_CLASS=TransactionFactory)
        data.update({
            **{
                'from_': generate_address(),
                'to': generate_address()
            },
            **block_data,
            **kwargs
        })
        data.pop('address', None)

        if 'from' in data:
            data.pop('from', None)
        else:
            data['from_'] = data.pop('from', None)

        if as_dict:
            return [
                factory.build(dict, FACTORY_CLASS=TransactionFactory, **{'address': data['from_'], **data}),
                factory.build(dict, FACTORY_CLASS=TransactionFactory, **{'address': data['to'], **data})
            ]
        return [
            cls.create(address=data['from_'], **data),
            cls.create(address=data['to'], **data),
        ]


@pytest.fixture()
def transaction_factory():
    return TransactionFactory
