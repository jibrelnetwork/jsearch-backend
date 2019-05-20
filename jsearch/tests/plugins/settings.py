import pytest


@pytest.fixture
def override_settings(mocker):
    def inner(name, value):
        mocker.patch(f'jsearch.settings.{name}', value)

    return inner
