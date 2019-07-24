import os
import click
from collections import Counter

from sqlalchemy import create_engine, MetaData, and_, select

from jsearch.common.tables import assets_summary_t, token_transfers_t, transactions_t


@click.command()
def test_tx_number():
    engine = create_engine(os.environ['JSEARCH_MAIN_DB'])

    meta = MetaData()
    meta.reflect(bind=engine)
    click.echo('DB reflected')

    q = assets_summary_t.select().where(assets_summary_t.c.asset_address == '')
    eth_assets = engine.execute(q).fetchall()

    q = assets_summary_t.select().where(assets_summary_t.c.asset_address != '')
    erc_assets = engine.execute(q).fetchall()

    q = transactions_t.select().where(and_(transactions_t.c.value != '0x0', transactions_t.c.is_forked == False))
    txs = engine.execute(q).fetchall()

    q = token_transfers_t.select().where(token_transfers_t.c.is_forked == False)
    erc = engine.execute(q).fetchall()

    txs_counter = Counter([t.address for t in txs])
    erc_counter = Counter([(t.address, t.token_address) for t in erc])

    print('ETH ASSETS')
    for a in eth_assets:
        if a.tx_number != txs_counter.get(a.address, 0):
            print(a.address, a.tx_number, txs_counter.get(a.address, 0))

    print('ERC ASSETS')
    for a in erc_assets:
        if a.tx_number != erc_counter.get((a.address, a.asset_address), 0):
            print(a.address, a.tx_number, erc_counter.get(a.address, 0))
    print('DONE')


if __name__ == '__main__':
    test_tx_number()