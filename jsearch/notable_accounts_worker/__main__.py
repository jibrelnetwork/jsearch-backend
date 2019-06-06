#!/usr/bin/env python

import click

from jsearch import settings
from jsearch.common import logs, worker
from jsearch.notable_accounts_worker import services


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL, help="Log level")
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
def main(log_level: str, no_json_formatter: bool) -> None:
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))

    worker.Worker(
        services.NotableAccountsService(),
        services.ApiService(),
    ).execute_from_commandline()


if __name__ == '__main__':
    main()
