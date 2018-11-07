
import pytest

from jsearch.common.testutils import setup_database, teardown_database


@pytest.fixture(scope="session", autouse=True)
def setup_database_fixture(request, pytestconfig):
    setup_database()
    request.addfinalizer(teardown_database)
