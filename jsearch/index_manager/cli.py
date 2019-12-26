import asyncio

import click

from jsearch import settings
from .manager import IndexManager


@click.group()
@click.pass_context
def indexes(ctx):
    """
    Index manager
    """
    ctx.obj = IndexManager(settings.JSEARCH_MAIN_DB)


@indexes.command()
@click.pass_obj
def inspect(mgr):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mgr.inspect())


@indexes.command()
@click.pass_obj
def status(mgr):
    click.echo('DB indexes status:\n===========================')
    loop = asyncio.get_event_loop()
    statuses = loop.run_until_complete(mgr.get_indexes_status())
    for status in statuses:
        click.echo('{name:70} {status:10} [{size}]'.format(**status))
    click.echo('=' * 80)
    click.echo('EXISTS {}, CREATING {}, NOT EXISTS {}'.format(
        len([s for s in statuses if s['status'] == 'EXISTS']),
        len([s for s in statuses if s['status'] == 'CREATING']),
        len([s for s in statuses if s['status'] == 'NOT EXISTS']),
    ))


@indexes.command()
@click.option('--dry-run', is_flag=True, help='Dry run - just print DROP INDEX statements, not actually run them in DB')
@click.pass_obj
def drop_all(mgr, dry_run):
    click.echo('Indexes to drop:')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mgr.drop_all(dry_run))


@indexes.command()
@click.option('--workers', '-w', default=2, help='Number of parallel workers - create your indexes faster!!!*')
@click.option('--dry-run', is_flag=True, help='Dry run - just print CREATE INDEX statements,'
                                              'not actually run them in DB')
@click.option('--concurrently', is_flag=True, help='use CREATE INDEX CONCURRENTLY PostgreSQL feature')
@click.option('--tables', '-t', default=None, help='Create index only for specified tables (comma separated list)')
@click.pass_obj
def create_all(mgr, workers, dry_run, concurrently, tables):
    click.echo('Creating DB indexes ({} workers):'.format(workers))
    loop = asyncio.get_event_loop()
    if tables:
        tables = [t.strip() for t in tables.split(',')]
    loop.run_until_complete(mgr.create_all(workers, dry_run, concurrently, tables))


@indexes.command()
@click.pass_obj
def vacuum_status(mgr):
    loop = asyncio.get_event_loop()
    status = loop.run_until_complete(mgr.get_vacuum_status())

    click.echo('==== VACUUMING STATUS ====\n')

    click.echo('XID status: {} {} {}'.format(*status['counter']))

    click.echo('\n\nVacuuming history:')
    click.echo('-' * 80)
    click.echo('table                            last_vacuum         last_autovacuum')
    click.echo('-' * 80)
    for item in status['history']:
        click.echo('{:32} {:20} {}'.format(item[0],
                                           item[1].strftime('%Y-%m-%d %H:%M') if item[1] else '',
                                           item[2].strftime('%Y-%m-%d %H:%M') if item[2] else ''))

    click.echo('\n\nVacuuming current progress:')
    click.echo('-' * 80)
    click.echo('table                            phase                       progress')
    click.echo('-' * 80)
    for item in status['progress']:
        click.echo('{:32} {:28} {}'.format(item[1],
                                           item[2],
                                           item[3]))
