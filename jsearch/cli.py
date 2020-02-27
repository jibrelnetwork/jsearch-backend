import logging

import click
from click import Context

from jsearch.api.cli import api
from jsearch.index_manager.cli import indexes
from jsearch.monitor.cli import monitor
from jsearch.structs import AppConfig
from jsearch.syncer.cli import syncer

logger = logging.getLogger(__name__)


@click.group()
@click.option('--log-level', envvar='LOG_LEVEL', help="Log level")
@click.option('--no-json-formatter', is_flag=True, envvar='NO_JSON_FORMATTER', help='Use default formatter')
@click.pass_context
def cli(ctx: Context, log_level: str, no_json_formatter: bool) -> None:
    ctx.obj = AppConfig(log_level, no_json_formatter)


cli.add_command(api)
cli.add_command(monitor)
cli.add_command(indexes)
cli.add_command(syncer)
