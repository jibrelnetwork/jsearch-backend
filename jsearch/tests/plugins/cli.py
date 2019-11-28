import click.testing
import pytest
from pytest_mock import MockFixture

import jsearch.common.worker


@pytest.fixture()
def cli_runner():
    return click.testing.CliRunner()


@pytest.fixture()
def _mock_loop_runners(mocker: MockFixture):
    mocker.patch.object(jsearch.common.worker.Worker, 'execute_from_commandline')
    mocker.patch('jsearch.api.cli.run_api')
