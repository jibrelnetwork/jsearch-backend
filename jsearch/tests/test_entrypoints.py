from typing import List

import click.testing
import pytest

from pytest_mock import MockFixture

import jsearch.common.last_block
import jsearch.common.worker
import jsearch.multiprocessing
import jsearch.pending_syncer.main
import jsearch.post_processing.__main__
import jsearch.syncer.main
import jsearch.validation.__main__
import jsearch.wallet_worker.__main__
import jsearch.worker.__main__

CODE_OK = 0
CODE_ERROR = 1
CODE_ERROR_FROM_CLICK = 2


@pytest.fixture()
def _mock_executor(mocker: MockFixture):
    mocker.patch.object(jsearch.multiprocessing.executor, 'init')


@pytest.fixture()
def _mock_loop_runners(mocker: MockFixture):
    mocker.patch.object(jsearch.common.worker.Worker, 'execute_from_commandline')
    mocker.patch.object(jsearch.validation.__main__, 'run')


@pytest.mark.usefixtures('_mock_executor', '_mock_loop_runners')
@pytest.mark.parametrize(
    'call_args, exit_code',
    [
        ([], CODE_ERROR_FROM_CLICK),
        (['invalid', 'set', 'of', 'args'], CODE_ERROR_FROM_CLICK),
        (['logs'], CODE_OK),
        (['transfers'], CODE_OK),
        (['logs', '--log-level', 'ERROR', '--no-json-formatter', '--workers', '1', '--mode', 'fast'], CODE_OK),
        (['transfers', '--log-level', 'ERROR', '--no-json-formatter', '--workers', '1', '--mode', 'fast'], CODE_OK),
    ],
    ids=[
        "no args",
        "invalid args",
        "required args logs",
        "required args transfers",
        "all args logs",
        "all args transfers",
    ]
)
def test_post_processing_entrypoint(
        cli_runner: click.testing.CliRunner,
        call_args: List[str],
        exit_code: int,
) -> None:
    result = cli_runner.invoke(jsearch.post_processing.__main__.main, call_args)
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
def test_syncer_entrypoint(
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
def test_pending_syncer_entrypoint(
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
        (['0x111', '--check-balances', '--show-holders', '--rewrite', '--log-level', 'ERROR', '--no-json-formatter'], CODE_OK),  # NOQA
    ],
    ids=[
        "no args",
        "invalid args",
        "required args",
        "all args",
    ]
)
def test_validation_entrypoint(
        mocker: MockFixture,
        cli_runner: click.testing.CliRunner,
        call_args: List[str],
        exit_code: int,
) -> None:

    mocker.patch.object(jsearch.validation.__main__, 'run')

    result = cli_runner.invoke(jsearch.validation.__main__.main, call_args)

    assert result.exit_code == exit_code


@pytest.mark.usefixtures('_mock_loop_runners')
@pytest.mark.parametrize(
    'call_args, exit_code',
    [
        ([], CODE_OK),
        (['invalid', 'set', 'of', 'args'], CODE_ERROR_FROM_CLICK),
        (['--log-level', 'ERROR', '--no-json-formatter'], CODE_OK),
    ],
    ids=[
        "no args",
        "invalid args",
        "all args",
    ]
)
def test_worker_entrypoint(
        cli_runner: click.testing.CliRunner,
        call_args: List[str],
        exit_code: int,
) -> None:
    result = cli_runner.invoke(jsearch.worker.__main__.main, call_args)
    assert result.exit_code == exit_code


@pytest.mark.usefixtures('_mock_loop_runners')
@pytest.mark.parametrize(
    'call_args, exit_code',
    [
        ([], CODE_OK),
        (['invalid', 'set', 'of', 'args'], CODE_ERROR_FROM_CLICK),
        (['--log-level', 'ERROR', '--no-json-formatter'], CODE_OK),
    ],
    ids=[
        "no args",
        "invalid args",
        "all args",
    ]
)
def test_wallet_worker_entrypoint(
        cli_runner: click.testing.CliRunner,
        call_args: List[str],
        exit_code: int,
) -> None:
    result = cli_runner.invoke(jsearch.wallet_worker.__main__.main, call_args)
    assert result.exit_code == exit_code
