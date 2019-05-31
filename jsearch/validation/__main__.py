# !/usr/bin/env python
import asyncio
import logging

import click

from jsearch import settings
from jsearch.common import logs
from jsearch.service_bus import service_bus
from jsearch.validation.balances import check_token_holder_balances, show_statistics, show_top_holders
from jsearch.validation.proxy import TokenProxy

logger = logging.getLogger(__name__)


def run(token: str, check_balances: bool, rewrite: bool, show_holders: bool) -> None:
    loop = asyncio.get_event_loop()

    tokens = service_bus.get_contracts(addresses=[token])
    if tokens:
        token = tokens[0]

        token_proxy = TokenProxy(abi=token['abi'], address=token['address'])
        loop.run_until_complete(show_statistics(token_proxy))

        if check_balances:
            loop.run_until_complete(check_token_holder_balances(token=token_proxy, rewrite_invalide_values=rewrite))

        if show_holders:
            loop.run_until_complete(show_top_holders(token=token_proxy))
    else:
        logger.info('Token was not found')


@click.command()
@click.argument('token')
@click.option('--check-balances', is_flag=True)
@click.option('--show-holders', is_flag=True)
@click.option('--rewrite', is_flag=True)
@click.option('--log-level', default=settings.LOG_LEVEL, help="Log level")
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
def main(
        token: str,
        check_balances: bool,
        rewrite: bool,
        show_holders: bool,
        log_level: str,
        no_json_formatter: bool,
) -> None:

    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))
    run(token, check_balances, rewrite, show_holders)


if __name__ == '__main__':
    main()
