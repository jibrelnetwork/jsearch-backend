from typing import Generator

import pytest
from aioresponses import aioresponses


@pytest.fixture()
def mock_aiohttp() -> Generator[aioresponses, None, None]:
    with aioresponses() as mock:
        yield mock
