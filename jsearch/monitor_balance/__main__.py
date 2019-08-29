# !/usr/bin/env python
import asyncio
import logging

import click

from jsearch import settings
from jsearch.common import logs

logger = logging.getLogger(__name__)


async def sleep():
    """
    your data migration could be here
    """
    while True:
        try:
            await asyncio.sleep(1)
        except KeyboardInterrupt:
            break


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL, help="Log level")
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
def main(
        log_level: str,
        no_json_formatter: bool,
) -> None:
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))

    asyncio.run(sleep())


if __name__ == '__main__':
    main()
