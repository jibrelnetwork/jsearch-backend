import pytest
from aiohttp.test_utils import TestClient
from pytest_mock import MockFixture

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')


async def test_healthcheck_everything_is_ok(mocker: MockFixture, cli: TestClient):
    mocker.patch('jsearch.common.utils.get_loop_tasks_count', return_value=9999)

    result = await cli.get('/healthcheck', json=dict())

    assert result.status == 200
    assert await result.json() == {
        "healthy": True,
        "isMainDbHealthy": True,
        "isNodeHealthy": True,
        "isLoopHealthy": True,
    }
