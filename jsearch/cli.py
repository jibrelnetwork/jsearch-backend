import logging

import click
from click import Context
from typing import NamedTuple

from jsearch.api.cli import api

logger = logging.getLogger(__name__)


class AppConfig(NamedTuple):
    log_level: str
    no_json_formatter: bool


@click.group()
@click.option('--log-level', envvar='LOG_LEVEL', help="Log level")
@click.option('--no-json-formatter', is_flag=True, envvar='NO_JSON_FORMATTER', help='Use default formatter')
@click.pass_context
def cli(ctx: Context, log_level: str, no_json_formatter: bool) -> None:
    ctx.obj = AppConfig(log_level, no_json_formatter)


cli.add_command(api)
