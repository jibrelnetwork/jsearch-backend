# !/usr/bin/env python
import asyncio
import logging

import click

from jsearch.common import logs
from jsearch.common.integrations.contracts import get_contract
from jsearch.validation.balances import check_token_holder_balances, show_statistics

logger = logging.getLogger(__name__)

VALIDATE_BALANCES = 'balances'


@click.command()
@click.argument('token')
@click.option('--check-balances', is_flag=True)
@click.option('--log-level', default='INFO', help="Log level")
def check(token, check_balances, log_level):
    logs.configure(log_level)
    loop = asyncio.get_event_loop()

    token = get_contract(token)
    if token:
        loop.run_until_complete(show_statistics(token))
        if check_balances:
            loop.run_until_complete(check_token_holder_balances(token))
    else:
        logger.info('Token was not found')


if __name__ == '__main__':
    check()
