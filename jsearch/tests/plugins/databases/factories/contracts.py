from random import randint
from typing import NamedTuple

import factory
import pytest

from jsearch.common.contracts import ERC20_ABI
from jsearch.tests.plugins.databases.factories.common import generate_address
from jsearch.typing import Abi


class Token(NamedTuple):
    abi: Abi

    address: str

    token_decimals: int
    token_name: str
    token_symbol: str


class TokenFactory(factory.Factory):
    abi = ERC20_ABI

    address = factory.LazyFunction(generate_address)
    token_decimals = factory.LazyFunction(lambda: randint(10, 18))
    token_name = factory.Faker('cryptocurrency_name', locale='en_US')
    token_symbol = factory.Faker('cryptocurrency_code', locale='en_US')

    class Meta:
        model = Token


@pytest.fixture()
def token_factory():
    return TokenFactory
