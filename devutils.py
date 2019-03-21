import click

from sqlalchemy import create_engine, MetaData, and_, select


@click.command()
@click.option('--src', required=True, help='DB DSN to copy from')
@click.option('--dst', required=True, help='DB DSN to copy to')
@click.option('--block-from', required=True, help='block numbers to start copy', type=int)
@click.option('--block-to', required=True, help='blocks number to end copy', type=int)
def copy_raw_db_sample(src, dst, block_from, block_to):
    source_engine = create_engine(src)
    dst_engine = create_engine(dst)

    meta = MetaData()
    meta.reflect(bind=source_engine)
    click.echo('SRC reflected')
    sample_data = {}

    meta.create_all(bind=dst_engine)
    click.echo('DST created')
    for t in meta.sorted_tables:
        if t.name in ('goose_db_version', 'chain_splits', 'pending_transactions'):
            continue
        table_data = source_engine.execute(t.select().where(and_(t.c.block_number >= block_from, t.c.block_number <= block_to))).fetchall()
        dst_engine.execute(t.insert(), *table_data)
        sample_data[t.name] = table_data
    reorgs_t = meta.tables['reorgs']
    splits_t = meta.tables['chain_splits']
    splits_ids = [r[0] for r in dst_engine.execute(select([reorgs_t.c.split_id]).distinct()).fetchall()]
    click.echo(f'splits ids {splits_ids}')
    splits = source_engine.execute(splits_t.select().where(splits_t.c.id.in_(splits_ids))).fetchall()
    dst_engine.execute(splits_t.insert(), *splits)


if __name__ == '__main__':
    copy_raw_db_sample()