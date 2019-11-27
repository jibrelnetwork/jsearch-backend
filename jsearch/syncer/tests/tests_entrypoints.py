import click.testing
import pytest
from typing import List

import jsearch.syncer.main
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
