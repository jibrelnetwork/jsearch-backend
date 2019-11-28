import click.testing
import pytest
from pytest_mock import MockFixture
from typing import List, Dict

import jsearch.cli
import jsearch.common.worker
import jsearch.pending_syncer.main
import jsearch.syncer.main
from jsearch.tests.common import CODE_OK, CODE_ERROR_FROM_CLICK

pytestmark = [pytest.mark.asyncio]


@pytest.mark.usefixtures('_mock_loop_runners')
@pytest.mark.parametrize(
    'call_args, exit_code',
    [
        ([], CODE_OK),
        (['invalid', 'set', 'of', 'args'], CODE_ERROR_FROM_CLICK),
        (["--log-level", 'ERROR', 'api'], CODE_OK),
        (['--no-json-formatter', 'api'], CODE_OK),
        (['api'], CODE_OK),
        (['api', '--port', '9000'], CODE_OK),
        (["--log-level", 'ERROR', '--no-json-formatter', 'api', '--port', '9000'], CODE_OK),
    ],
    ids=[
        "no args",
        "invalid args",
        "log level",
        "no json formatter",
        "api",
        "api port",
        "all args",
    ]
)
async def test_jsearch_cli(
        cli_runner: click.testing.CliRunner,
        call_args: List[str],
        exit_code: int,
) -> None:
    result = cli_runner.invoke(jsearch.cli.cli, call_args)
    assert result.exit_code == exit_code


@pytest.mark.usefixtures('_mock_loop_runners')
@pytest.mark.parametrize(
    'call_args, exit_code',
    [
        ([], CODE_OK),
        (['invalid', 'set', 'of', 'args'], CODE_ERROR_FROM_CLICK),
        (['--log-level', 'ERROR', '--no-json-formatter', '--sync-range', '26000-27000'], CODE_OK),
    ],
    ids=[
        "no args",
        "invalid args",
        "all args",
    ]
)
async def test_pending_syncer_entrypoint(
        cli_runner: click.testing.CliRunner,
        call_args: List[str],
        exit_code: int,
) -> None:
    result = cli_runner.invoke(jsearch.pending_syncer.main.run, call_args)
    assert result.exit_code == exit_code


@pytest.mark.usefixtures('_mock_loop_runners')
@pytest.mark.parametrize(
    'env, expected',
    [
        ({'SYNC_RANGE': '1000-2000'}, {'sync_range': '1000-2000'}),
    ],
    ids=[
        "sync-range",
    ]
)
async def test_pending_syncer_env_kwargs(
        cli_runner: click.testing.CliRunner,
        mocker: MockFixture,
        env: Dict[str, str],
        expected: Dict[str, str],
) -> None:
    kwargs = {}
    mocker.patch('jsearch.pending_syncer.main.run.invoke', lambda ctx: kwargs.update(ctx.params))
    result = cli_runner.invoke(jsearch.pending_syncer.main.run, env=env)

    assert {key: value for key, value in kwargs.items() if key in expected} == expected
    assert result.exit_code == CODE_OK
