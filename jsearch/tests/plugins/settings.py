import pytest
from typing import Any


@pytest.fixture
def override_settings(mocker):
    def inner(name: str, value: Any) -> None:
        mocker.patch(f'jsearch.settings.{name}', value)

    return inner
