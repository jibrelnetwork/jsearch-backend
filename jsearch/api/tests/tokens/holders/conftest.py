from typing import Callable

import pytest
import yarl

from jsearch.tests.plugins.databases.factories.common import generate_address


@pytest.fixture
def token_address() -> str:
    return generate_address()


@pytest.fixture()
def url(token_address) -> yarl.URL:
    return yarl.URL(f"/v1/tokens/{token_address}/holders")


@pytest.fixture()
def create_token_holders(
        token_holder_factory
) -> Callable[[str], None]:
    def create_env(token_address: str,
                   balances=(100, 200, 300),
                   holders_per_balance=2) -> None:

        for balance in balances:
            for _ in range(holders_per_balance):
                token_holder_factory.create(balance=balance, token_address=token_address)

    return create_env
