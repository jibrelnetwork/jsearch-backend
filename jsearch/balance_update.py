import os

import click

from sqlalchemy import create_engine, MetaData, and_, select

from jsearch.syncer.processor import SyncProcessor


@click.command()
def update_balances(src, dst, block_from, block_to):
    q = "SELECT DISTINCT address FROM token_transfers WHERE block_number > 6999999;"

    processor = SyncProcessor()

    processor.




if __name__ == '__main__':
    update_balances()