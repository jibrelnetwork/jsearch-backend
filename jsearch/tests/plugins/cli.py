import click.testing
import pytest


@pytest.fixture()
def cli_runner():
    return click.testing.CliRunner()
