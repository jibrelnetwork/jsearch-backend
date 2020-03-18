from typing import Callable

import pytest
import yarl
from aiohttp import web


@pytest.fixture()
def get_url(app: web.Application) -> Callable[[str], yarl.URL]:
    def get_url(token_address: str) -> yarl.URL:
        return app.router['dex_history'].url_for(token_address=token_address)

    return get_url


@pytest.fixture()
def url(get_url: Callable[[str], yarl.URL], token_address: str) -> yarl.URL:
    return get_url(token_address)
