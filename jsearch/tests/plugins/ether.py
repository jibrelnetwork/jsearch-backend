import random
import string
from typing import Generator

import pytest


@pytest.fixture()
def ether_address_generator() -> Generator[str, None, None]:
    def generator() -> Generator[str, None, None]:
        while True:
            alphabet = string.digits + "abcde"
            body = "".join([random.choice(alphabet) for _ in range(0, 40)])
            yield f"0x{body}"

    return generator()
