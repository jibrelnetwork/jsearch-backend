import asyncio
import logging

import click
from aiopg.sa import create_engine

from jsearch import settings
from jsearch.common import logs, stats
from jsearch.common import worker
from jsearch.syncer import services
from jsearch.syncer.manager import SYNCER_BALANCE_MODE_LATEST, SYNCER_BALANCE_MODE_OFFSET
from jsearch.utils import parse_range

logger = logging.getLogger("syncer")


# ToDo: Remove after release 1.2
async def wait_new_scheme():
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'token_holders' AND column_name = 'id' and data_type = 'bigint';
    """
    engine = await create_engine(dsn=settings.JSEARCH_MAIN_DB, maxsize=1)
    while True:
        try:
            async with engine.acquire() as connection:
                async with connection.execute(query) as cursor:
                    row = await cursor.fetchone()
                    if row is None:
                        logger.info('Wait new scheme, wait 5 seconds...')
                        await asyncio.sleep(5)
                    else:
                        logger.info('New schema have founded, start sync...')
                        break
        except KeyboardInterrupt:
            break
    engine.close()


def wait():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(wait_new_scheme())


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL, help="Log level")
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
@click.option('--sync-range', default=None, help="Blocks range to sync")
@click.option(
    '--balance-mode',
    type=click.Choice(choices=[SYNCER_BALANCE_MODE_LATEST, SYNCER_BALANCE_MODE_OFFSET]),
    default=SYNCER_BALANCE_MODE_LATEST
)
def run(log_level, no_json_formatter, sync_range, balance_mode):
    stats.setup_syncer_metrics()
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))

    wait()

    syncer = services.SyncerService(
        sync_range=parse_range(sync_range),
        balance_mode=balance_mode
    )
    syncer.add_dependency(services.ApiService())

    worker.Worker(syncer).execute_from_commandline()


if __name__ == '__main__':
    run()
