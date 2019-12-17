from aiohttp.test_utils import TestClient
from pytest_mock import MockFixture


async def test_healthcheck_everything_is_ok(
        mocker: MockFixture,
        cli: TestClient,
        override_settings,
):
    # There's no `/healthcheck` in the spec.
    cli.app['validate_spec'] = False

    override_settings('VERSION', '1.0.0-app-version')
    mocker.patch('jsearch.common.utils.get_loop_tasks_count', return_value=9999)

    result = await cli.get('/healthcheck', json=dict())

    assert result.status == 200
    assert await result.json() == {
        "healthy": True,
        "version": '1.0.0-app-version',
        "isMainDbHealthy": True,
        "isNodeHealthy": True,
        "isLoopHealthy": True,
    }
