import click

from jsearch.common import logs, worker
from jsearch.token_holders_cleaner import service, settings


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL, help="Log level")
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
def run(log_level, no_json_formatter):
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))

    worker.Worker(
        service.TokenHoldersCleaner(
            main_db_dsn=settings.JSEARCH_MAIN_DB,
        ),
    ).execute_from_commandline()


if __name__ == '__main__':
    run()
