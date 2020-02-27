from typing import List, Dict

import click.testing
import pytest

import jsearch.cli
import jsearch.syncer.cli
from jsearch.tests.common import CODE_OK, CODE_ERROR_FROM_CLICK


@pytest.mark.usefixtures('_mock_loop_runners')
@pytest.mark.parametrize(
    'call_args, exit_code',
    [
        (['syncer'], CODE_OK),
        (['syncer', 'invalid', 'set', 'of', 'args'], CODE_ERROR_FROM_CLICK),
        (['--log-level', 'ERROR', '--no-json-formatter', 'syncer'], CODE_OK),
        (['syncer', '--sync-range', '26000-27000'], CODE_OK),
        (['syncer', '-r', '26000-27000'], CODE_OK),
        (['syncer', '--resync', "0"], CODE_OK),
        (['syncer', '--resync', "1"], CODE_OK),
        (['syncer', '--resync', "0", '--resync-chain-splits', "0"], CODE_OK),
        (['syncer', '--resync', "0", '--resync-chain-splits', "1"], CODE_OK),
        (['syncer', '--sync-range', '26000-27000', '--resync', 'false'], CODE_OK),
    ],
    ids=[
        "no-args",
        "invalid-args",
        "log-level-debug-no-json-formatter",
        "sync-range",
        "sync-range-(short)",
        "resync-true",
        "resync-false",
        "resync-false-resync-chain-split-true",
        "resync-false-resync-chain-split-false",
        "all args",
    ]
)
async def test_syncer_entrypoint(
        cli_runner: click.testing.CliRunner,
        call_args: List[str],
        exit_code: int,
) -> None:
    result = cli_runner.invoke(jsearch.cli.cli, call_args)
    assert result.exit_code == exit_code, result


class MockFixture(object):
    pass


@pytest.mark.usefixtures('_mock_loop_runners')
@pytest.mark.parametrize(
    'call_args, exit_code',
    [
        ([], CODE_OK),
        (['monitor', 'd'], CODE_ERROR_FROM_CLICK),
        (['monitor', '--port', '8000'], CODE_OK),
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
async def test_monitor_entrypoint(
        cli_runner: click.testing.CliRunner,
        call_args: List[str],
        exit_code: int,
) -> None:
    result = cli_runner.invoke(jsearch.cli.cli, call_args)
    assert result.exit_code == exit_code, result


@pytest.mark.usefixtures('_mock_loop_runners')
@pytest.mark.parametrize(
    'env, expected',
    [
        ({'SYNC_RANGE': '1000-2000'}, {'sync_range': '1000-2000'}),
        ({'SYNCER_WORKERS': '15'}, {'workers': 15}),
        ({'SYNCER_RESYNC': '0'}, {'resync': False}),
        ({'SYNCER_RESYNC': '1'}, {'resync': True}),
        ({'SYNCER_RESYNC_CHAIN_SPLITS': '0'}, {'resync_chain_splits': False}),
        ({'SYNCER_RESYNC_CHAIN_SPLITS': '1'}, {'resync_chain_splits': True}),

    ],
    ids=[
        "sync-range",
        "syncer-workers",
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
    mocker.patch('jsearch.syncer.cli.syncer.invoke', lambda ctx: kwargs.update(ctx.params))
    result = cli_runner.invoke(jsearch.syncer.cli.syncer, env=env)

    assert {key: value for key, value in kwargs.items() if key in expected} == expected
    assert result.exit_code == CODE_OK
