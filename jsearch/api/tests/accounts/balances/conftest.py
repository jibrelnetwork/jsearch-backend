from typing import Callable, List

import pytest
import yarl
from aiohttp import web

from jsearch.tests.plugins.databases.factories.common import generate_address


@pytest.fixture()
def get_url(app: web.Application) -> Callable[[List[str]], yarl.URL]:
    def get_url(addresses: List[str]) -> yarl.URL:
        url = app.router['accounts_balances'].url_for()
        if addresses:
            url = url.with_query({'addresses': ','.join(addresses)})
        return url

    return get_url


@pytest.fixture()
def account_address():
    return generate_address()


@pytest.fixture()
def url(get_url: Callable[[List[str]], yarl.URL], account_address: str) -> yarl.URL:
    return get_url([account_address])
