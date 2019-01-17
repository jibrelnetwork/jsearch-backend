# !/usr/bin/env python
import asyncio
import logging

import click

from jsearch.common import logs
from jsearch.common.integrations.contracts import get_contract
from jsearch.validation.balances import check_token_holder_balances, show_statistics, show_top_holders
from jsearch.validation.proxy import TokenProxy

logger = logging.getLogger(__name__)


@click.command()
@click.argument('token')
@click.option('--check-balances', is_flag=True)
@click.option('--show-holders', is_flag=True)
@click.option('--show-stats', is_flag=True)
@click.option('--rewrite', is_flag=True)
@click.option('--limit', type=int)
@click.option('--log-level', default='INFO', help="Log level")
def check(token, check_balances, rewrite, show_holders, show_stats, limit, log_level):
    logs.configure(log_level)
    loop = asyncio.get_event_loop()

    token = get_contract(token)
    if token:
        token_proxy = TokenProxy(abi=token['abi'], address=token['address'])
        if show_stats:
            loop.run_until_complete(show_statistics(token_proxy))

        if check_balances:
            loop.run_until_complete(check_token_holder_balances(token=token_proxy, rewrite_invalide_values=rewrite))

        if show_holders:
            loop.run_until_complete(show_top_holders(token=token_proxy, limit=limit))
    else:
        logger.info('Token was not found')


if __name__ == '__main__':
    check()
