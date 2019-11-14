from typing import Type

import factory
import pytest

from jsearch.tests.plugins.databases.factories.common import generate_address, generate_psql_timestamp


class _RawPendingTransactionFieldsFactory(factory.DictFactory):
    r = factory.LazyFunction(generate_address)
    s = factory.LazyFunction(generate_address)
    v = factory.LazyFunction(generate_address)
    to = factory.LazyFunction(generate_address)
    gas = factory.LazyFunction(generate_address)
    from_ = factory.LazyFunction(generate_address)
    hash = factory.LazyFunction(generate_address)
    input = factory.LazyFunction(generate_address)
    nonce = factory.LazyFunction(generate_address)
    value = factory.LazyFunction(generate_address)
    gas_price = factory.LazyFunction(generate_address)

    class Meta:
        rename = {
            'from_': 'from',
            'gas_price': 'gasPrice',
        }


class RawPendingTransactionFactory(factory.DictFactory):
    tx_hash = factory.LazyFunction(generate_address)
    status = None
    id = factory.Faker('pyint')
    timestamp = factory.LazyFunction(generate_psql_timestamp)
    removed = factory.Faker('pybool')
    node_id = factory.LazyFunction(generate_address)
    created_at = factory.LazyFunction(generate_psql_timestamp)

    fields = factory.SubFactory(_RawPendingTransactionFieldsFactory, hash=factory.SelfAttribute('..tx_hash'))


@pytest.fixture
def raw_pending_transaction_factory() -> Type[RawPendingTransactionFactory]:
    return RawPendingTransactionFactory
