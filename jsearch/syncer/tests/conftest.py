import pytest

from jsearch.common.testutils import setup_database, teardown_database


def pytest_runtestloop(session):
    print("SESS:", session)


@pytest.fixture(scope="session", autouse=True)
def my_own_session_run_at_beginning(request):
    print('\nIn my_own_session_run_at_beginning()')
    request.addfinalizer(my_own_session_run_at_end)


def my_own_session_run_at_end():
    print('In my_own_session_run_at_end()')


@pytest.fixture(scope="session", autouse=True)
def setup_database_fixture(request, pytestconfig):
    setup_database()
    request.addfinalizer(teardown_database)
