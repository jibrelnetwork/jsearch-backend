#!/usr/bin/env python

import click

from jsearch import settings
from jsearch.common import logs, worker
from jsearch.notable_accounts_worker import services


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL, help='Log level')
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
@click.option('--update-if-exists', is_flag=True, default=settings.NOTABLE_ACCOUNT_UPDATE_IF_EXISTS, help='Update notable account, if already in DB')  # NOQA: 501
def main(log_level: str, no_json_formatter: bool, update_if_exists: bool) -> None:
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))

    worker.Worker(
        services.NotableAccountsService(
            db_dsn=settings.JSEARCH_MAIN_DB,
            update_if_exists=update_if_exists,
        ),
        services.ApiService(),
    ).execute_from_commandline()


if __name__ == '__main__':
    main()
