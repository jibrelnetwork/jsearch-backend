import click.testing
import pytest
from typing import List, Dict

import jsearch.syncer.main
import jsearch.cli
from jsearch.tests.common import CODE_OK, CODE_ERROR_FROM_CLICK


@pytest.mark.usefixtures('_mock_loop_runners')
@pytest.mark.parametrize(
    'call_args, exit_code',
    [
        ([], CODE_OK),
        (['invalid', 'set', 'of', 'args'], CODE_ERROR_FROM_CLICK),
        (['--log-level', 'ERROR', '--no-json-formatter', '--sync-range', '26000-27000'], CODE_OK),
        (['--log-level', 'ERROR', '--no-json-formatter', '--resync', 0], CODE_OK),
        (['--log-level', 'ERROR', '--no-json-formatter', '--resync', 1], CODE_OK),
        (['--log-level', 'ERROR', '--no-json-formatter', '--resync', 0, '--resync-chain-splits', 0], CODE_OK),
        (['--log-level', 'ERROR', '--no-json-formatter', '--resync', 0, '--resync-chain-splits', 1], CODE_OK),
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


class MockFixture(object):
    pass


@pytest.mark.usefixtures('_mock_loop_runners')
@pytest.mark.parametrize(
    'call_args, exit_code',
    [
        ([], CODE_OK),
        (['monitor', 'd'], CODE_ERROR_FROM_CLICK),
        (['monitor', 'port', '8000'], CODE_OK),
        (['--log-level', 'ERROR', '--no-json-formatter', 'monitor'], CODE_OK),
        (['--no-json-formatter', 'monitor'], CODE_OK),
    ],
    ids=[
        "no args",
        "invalid args",
        "port",
        "log-level",
        "no-json-formatter"
    ]
)
async def test_lag_checker_entrypoint(
        cli_runner: click.testing.CliRunner,
        call_args: List[str],
        exit_code: int,
) -> None:
    result = cli_runner.invoke(jsearch.cli.cli, call_args)
    assert result.exit_code == exit_code


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
