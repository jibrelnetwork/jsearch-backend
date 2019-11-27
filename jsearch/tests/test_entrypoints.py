import click.testing
import pytest
from pytest_mock import MockFixture
from typing import List, Dict

import jsearch.cli
import jsearch.common.worker
import jsearch.monitor_balance.__main__
import jsearch.pending_syncer.main
import jsearch.syncer.main

CODE_OK = 0
CODE_ERROR = 1
CODE_ERROR_FROM_CLICK = 2

pytestmark = [pytest.mark.asyncio]


@pytest.fixture()
def _mock_loop_runners(mocker: MockFixture):
    mocker.patch.object(jsearch.common.worker.Worker, 'execute_from_commandline')
    mocker.patch.object(jsearch.monitor_balance.__main__, 'main')
    mocker.patch('jsearch.api.cli.run_api')


@pytest.mark.usefixtures('_mock_loop_runners')
@pytest.mark.parametrize(
    'call_args, exit_code',
    [
        ([], CODE_OK),
        (['invalid', 'set', 'of', 'args'], CODE_ERROR_FROM_CLICK),
        (['--log-level', 'ERROR', '--no-json-formatter', '--sync-range', '26000-27000'], CODE_OK),
        (['--log-level', 'ERROR', '--no-json-formatter', '--resync', 'true'], CODE_OK),
        (['--log-level', 'ERROR', '--no-json-formatter', '--resync', 'false'], CODE_OK),
        (['--log-level', 'ERROR', '--no-json-formatter', '--sync-range',
          '26000-27000', '--resync', 'false'], CODE_OK),
    ],
    ids=[
        "no args",
        "invalid args",
        "sync range",
        "resync true",
        "resync false",
        "all args",
    ]
)
async def test_syncer_entrypoint(
        cli_runner: click.testing.CliRunner,
        call_args: List[str],
        exit_code: int,
) -> None:
    result = cli_runner.invoke(jsearch.syncer.main.run, call_args)
    assert result.exit_code == exit_code


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


@pytest.mark.usefixtures('_mock_loop_runners')
@pytest.mark.parametrize(
    'env, expected',
    [
        ({'SYNC_RANGE': '1000-2000'}, {'sync_range': '1000-2000'}),
        ({'SYNCER_WORKERS': '15'}, {'workers': 15}),
        ({'SYNCER_CHECK_LAG': '0'}, {'check_lag': False}),
        ({'SYNCER_CHECK_LAG': '1'}, {'check_lag': True}),
        ({'SYNCER_CHECK_HOLES': '0'}, {'check_holes': False}),
        ({'SYNCER_CHECK_HOLES': '1'}, {'check_holes': True}),
        ({'SYNCER_RESYNC': '0'}, {'resync': False}),
        ({'SYNCER_RESYNC': '1'}, {'resync': True}),
        ({'SYNCER_RESYNC_CHAIN_SPLITS': '0'}, {'resync_chain_splits': False}),
        ({'SYNCER_RESYNC_CHAIN_SPLITS': '1'}, {'resync_chain_splits': True}),

    ],
    ids=[
        "sync-range",
        "syncer-workers",
        "syncer-check-lag-false",
        "syncer-check-lag-true",
        "syncer-check-holes-false",
        "syncer-check-holes-true",
        "syncer-resync-false",
        "syncer-resync-true",
        "syncer-resync-chain-splits-false",
        "syncer-resync-chain-splits-true",
    ]
)
async def test_syncer_env_kwargs(
        cli_runner: click.testing.CliRunner,
        mocker: MockFixture,
        env: Dict[str, str],
        expected: Dict[str, str],
) -> None:
    kwargs = {}
    mocker.patch('jsearch.syncer.main.run.invoke', lambda ctx: kwargs.update(ctx.params))
    result = cli_runner.invoke(jsearch.syncer.main.run, env=env)

    assert {key: value for key, value in kwargs.items() if key in expected} == expected
    assert result.exit_code == CODE_OK


@pytest.mark.usefixtures('_mock_loop_runners')
@pytest.mark.parametrize(
    'call_args, exit_code',
    [
        ([], CODE_OK),
        (['--offset', '20', '--block', '20000'], CODE_OK),
    ],
    ids=[
        "no args",
        "all args",
    ]
)
async def test_monitor_balance_entrypoint(
        mocker: MockFixture,
        cli_runner: click.testing.CliRunner,
        call_args: List[str],
        exit_code: int,
) -> None:
    mocker.patch.object(jsearch.monitor_balance.__main__, 'main')

    result = cli_runner.invoke(jsearch.monitor_balance.__main__.main, call_args)

    assert result.exit_code == exit_code
