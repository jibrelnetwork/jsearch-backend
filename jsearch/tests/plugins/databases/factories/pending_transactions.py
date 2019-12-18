from random import randint

from typing import Optional, Any

import factory
import pytest
from eth_utils import remove_0x_prefix

from jsearch.common.contracts import ERC20_METHODS_IDS
from jsearch.common.tables import pending_transactions_t
from jsearch.tests.plugins.databases.factories.common import generate_address, generate_psql_timestamp
from .common import session, Base


class PendingTransactionModel(Base):
    __table__ = pending_transactions_t


class PendingTransactionFactory(factory.alchemy.SQLAlchemyModelFactory):
    last_synced_id = factory.Faker('pyint')  # 0..9999
    id = factory.Sequence(lambda n: n)  # 0..9999
    hash = factory.LazyFunction(generate_address)
    status = ''
    timestamp = factory.LazyFunction(generate_psql_timestamp)
    removed = False
    node_id = '1'
    r = '0xaa'
    s = '0xbb'
    v = '0xcc'
    to = factory.LazyFunction(generate_address)
    from_ = factory.LazyFunction(generate_address)
    gas = factory.Faker('pyint')
    gas_price = factory.Faker('pyint')
    input = '0x00'
    nonce = factory.Sequence(lambda n: n)
    value = factory.LazyFunction(lambda: str(randint(0, 999)))

    class Meta:
        model = PendingTransactionModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'
        rename = {'from_': 'from'}

    @classmethod
    def create_eth_transfer(cls, **kwargs: Any) -> PendingTransactionModel:
        return cls.create(**kwargs)

    @classmethod
    def create_token_transfer(
            cls,
            method_id: str,
            address_from: Optional[str] = None,
            address_to: Optional[str] = None,
            token_value: Optional[int] = None,
    ):
        if method_id not in {'transfer', 'transferFrom'}:
            raise ValueError(f"ERC20 method ID must be either 'transfer' or 'transferFrom', got '{method_id}'")

        transfer_sign = ERC20_METHODS_IDS[method_id]

        address_from = address_from or generate_address()
        address_to = address_to or generate_address()
        token_value = hex(token_value)

        address_from_unprefixed = remove_0x_prefix(address_from)
        address_to_unprefixed = remove_0x_prefix(address_to)
        token_value_unprefixed = remove_0x_prefix(token_value)

        if method_id == 'transfer':
            # signature is `function transferFrom(address to, uint tokens)`
            input_ = f"{transfer_sign}{address_to_unprefixed.zfill(64)}{token_value_unprefixed.zfill(64)}"
        else:
            # signature is `function transferFrom(address from, address to, uint tokens)`
            input_ = f"{transfer_sign}{address_from_unprefixed.zfill(64)}{address_to_unprefixed.zfill(64)}{token_value_unprefixed.zfill(64)}"  # NOQA: E501

        return cls.create(value="0x0", from_=address_from, input=input_)


@pytest.fixture()
def pending_transaction_factory():
    yield PendingTransactionFactory
    PendingTransactionFactory.reset_sequence()
