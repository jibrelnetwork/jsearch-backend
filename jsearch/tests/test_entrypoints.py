import click.testing
import pytest
from pytest_mock import MockFixture
from typing import List

import jsearch.common.worker
import jsearch.monitor_balance.__main__
import jsearch.multiprocessing
import jsearch.pending_syncer.main
import jsearch.syncer.main

CODE_OK = 0
CODE_ERROR = 1
CODE_ERROR_FROM_CLICK = 2

pytestmark = [pytest.mark.asyncio]


@pytest.fixture()
def _mock_executor(mocker: MockFixture):
    mocker.patch.object(jsearch.multiprocessing.executor, 'init')


@pytest.fixture()
def _mock_loop_runners(mocker: MockFixture):
    mocker.patch.object(jsearch.common.worker.Worker, 'execute_from_commandline')
    mocker.patch.object(jsearch.monitor_balance.__main__, 'main')


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
