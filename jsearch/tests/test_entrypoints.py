import click.testing
import pytest
from pytest_mock import MockFixture
from typing import List

import jsearch.common.worker
import jsearch.multiprocessing
import jsearch.pending_syncer.main
import jsearch.syncer.main
import jsearch.validation.__main__

CODE_OK = 0
CODE_ERROR = 1
CODE_ERROR_FROM_CLICK = 2

pytestmark = [pytest.mark.asyncio, pytest.mark.usefixtures('disable_metrics_setup')]


@pytest.fixture()
def _mock_executor(mocker: MockFixture):
    mocker.patch.object(jsearch.multiprocessing.executor, 'init')


@pytest.fixture()
def _mock_loop_runners(mocker: MockFixture):
    mocker.patch.object(jsearch.common.worker.Worker, 'execute_from_commandline')
    mocker.patch.object(jsearch.validation.__main__, 'run')


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
        ([], CODE_ERROR_FROM_CLICK),
        (['invalid', 'set', 'of', 'args'], CODE_ERROR_FROM_CLICK),
        (['0x111'], CODE_OK),
        (['0x111', '--check-balances', '--show-holders', '--rewrite', '--log-level', 'ERROR', '--no-json-formatter'],
         CODE_OK),  # NOQA
    ],
    ids=[
        "no args",
        "invalid args",
        "required args",
        "all args",
    ]
)
async def test_validation_entrypoint(
        mocker: MockFixture,
        cli_runner: click.testing.CliRunner,
        call_args: List[str],
        exit_code: int,
) -> None:
    mocker.patch.object(jsearch.validation.__main__, 'run')

    result = cli_runner.invoke(jsearch.validation.__main__.main, call_args)

    assert result.exit_code == exit_code
