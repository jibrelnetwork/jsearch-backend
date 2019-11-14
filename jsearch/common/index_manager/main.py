import asyncio
import logging
import re
import os

from psycopg2.extras import DictCursor
import aiopg
import yaml
import click

from jsearch import settings

logger = logging.getLogger('index_manager')
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s -- %(message)s',
                    handlers=[logging.StreamHandler()])

INDEX_RE = r'CREATE\s?(UNIQUE)? INDEX (\w+) ON public.(\w+) USING (\w+) (\([^\)]*\))\s?(WHERE)?\s?(\([^\)]*\))?'
DEFAULT_FILENAME = os.path.join(os.path.dirname(__file__), 'indexes.yaml')


class Index:
    name: str
    table: str
    fields: str
    type: str
    partial_condition: str
    pk: bool
    unique: bool
    indexdef: str

    def __init__(self, name, table, fields, type='btree', partial_condition='', pk=False, unique=False, indexdef=''):
        self.name = name
        self.table = table
        self.fields = fields
        self.type = type
        self.partial_condition = partial_condition
        self.pk = pk
        self.unique = unique
        self.indexdef = indexdef

    def get_drop_statement(self):
        if self.pk is False:
            stmt = """DROP INDEX IF EXISTS {}""".format(self.name)
        else:
            stmt = """ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}""".format(self.table, self.name)
        return stmt

    def get_create_statement(self, concurrently):
        if self.pk is False:
            options = {
                'name': self.name,
                'table': self.table,
                'fields': self.fields,
                'type': self.type,
                'unique': 'UNIQUE' if self.unique else '',
                'concurrently': 'CONCURRENTLY' if concurrently else '',
                'where': 'WHERE {}'.format(self.partial_condition) if self.partial_condition else '',
            }
            stmt = """CREATE {unique} INDEX {concurrently}
                      IF NOT EXISTS {name} ON {table} USING {type} {fields} {where}""".format(
                **options)
        else:
            stmt = """ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY {};""".format(self.table, self.name, self.fields)
        return stmt

    async def get_status(self, conn):

        size_q = """SELECT nspname || '.' || relname AS "relation",
                            pg_size_pretty(pg_relation_size(C.oid)) AS "size"
                    FROM pg_class C
                    LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
                    WHERE nspname = 'public' AND relname=%s;"""
        async with conn.cursor(cursor_factory=DictCursor) as cur:
            await cur.execute(size_q, [self.name])
            res = await cur.fetchone()
            if res is None:
                index_size = None
            else:
                index_size = res['size']

        stat_q = """SELECT * from pg_stat_user_indexes WHERE indexrelname=%s"""
        async with conn.cursor(cursor_factory=DictCursor) as cur:
            await cur.execute(stat_q, [self.name])
            res = await cur.fetchone()
            if res is not None:
                return {'name': self.name, 'status': 'EXISTS', 'size': index_size}

        act_q = """SELECT now() - query_start, state, wait_event
                       FROM pg_stat_activity WHERE (query like '%s' OR query like '%s') AND state <> 'idle'"""
        term = 'CREATE % INDEX% {} %'.format(self.name)
        term2 = 'ALTER TABLE {} ADD CONSTRAINT % {} %'.format(self.table, self.name)
        async with conn.cursor(cursor_factory=DictCursor) as cur:
            await cur.execute(act_q, [term, term2])
            res = await cur.fetchone()
            if res is not None:
                return {'name': self.name,
                        'status': 'CREATING',
                        'query_duration': res[0],
                        'query_state': res[1],
                        'query_wait': res[2],
                        'current_size': index_size or '0 bytes'}

        if index_size is None:
            return {'name': self.name, 'status': 'NOT EXISTS', 'size': 'N/A'}
        assert False, 'Index Status miss'


class IndexManager:
    indexes = None

    def __init__(self, db_connection_string):
        self.db_connection_string = db_connection_string
        self.load_indexes()
        self.queue = list()

    def load_indexes(self, filename=DEFAULT_FILENAME):
        items = yaml.load(open(filename, 'rb'), Loader=yaml.SafeLoader)
        self.indexes = [Index(**item) for item in items]

    async def connect(self):
        conn = await aiopg.connect(settings.JSEARCH_MAIN_DB, timeout=None)
        return conn

    async def worker(self, n):
        conn = await self.connect()
        logger.info('Worker %s STARTED', n)
        while True:
            try:
                cmd = self.queue.pop()
            except IndexError:
                logger.info('Worker %s WORK FINISHED', n)
                break
            async with conn.cursor() as cur:
                logger.info('Worker %s EXECUTE: %s', n, cmd)
                await cur.execute(cmd)
        conn.close()

    async def drop_all(self, dry_run=False):
        conn = await self.connect()
        for idx in self.indexes:
            stmt = idx.get_drop_statement()
            if dry_run is False:
                async with conn.cursor() as cur:
                    await cur.execute(stmt)
                logger.info(stmt)
            else:
                logger.info('/* dry run */ %s', stmt)
        conn.close()

    async def create_all(self, workers_number, dry_run, concurrently, tables):
        if tables:
            indexes = [i for i in self.indexes if i.table in tables]
        else:
            indexes = self.indexes
        for idx in indexes:
            stmt = idx.get_create_statement(concurrently)
            if dry_run is True:
                logger.info('/* dry run */ %s', stmt)
            self.queue.append(stmt)
        if dry_run is True:
            return
        tasks = []
        for n in range(workers_number):
            task = self.worker(n)
            tasks.append(task)
        await asyncio.gather(*tasks)

    async def get_indexes_status(self):
        conn = await self.connect()
        statuses = []
        for idx in self.indexes:
            status = await idx.get_status(conn)
            statuses.append(status)
        conn.close()
        return statuses

    async def inspect(self):
        conn = await self.connect()
        indexes = []
        try:
            q = """SELECT * FROM pg_indexes WHERE schemaname='public';"""
            async with conn.cursor(cursor_factory=DictCursor) as cur:
                await cur.execute(q)
                res = await cur.fetchall()
            for row in res:
                definition = parse_indexdef(row['indexdef'])
                indexes.append(definition)
            print(yaml.dump(indexes, default_flow_style=False, sort_keys=False, line_break='\n\n'))  # noqa: T001
        finally:
            conn.close()

    async def get_vacuum_status(self):
        conn = await self.connect()
        try:
            progress_q = """select p.pid, t.relname, p.phase, p.heap_blks_scanned/p.heap_blks_total::real
                            from pg_stat_progress_vacuum p inner join pg_stat_user_tables t on p.relid = t.relid;"""
            async with conn.cursor(cursor_factory=DictCursor) as cur:
                await cur.execute(progress_q)
                progress_result = await cur.fetchall()

            history_q = """SELECT relname, last_vacuum, last_autovacuum FROM pg_stat_user_tables;"""
            async with conn.cursor(cursor_factory=DictCursor) as cur:
                await cur.execute(history_q)
                history_result = await cur.fetchall()


        finally:
            conn.close()


def parse_indexdef(indexdef):
    m = re.match(INDEX_RE, indexdef)
    if m:
        g = m.groups()
        definition = {
            'name': g[1],
            'table': g[2],
            'fields': g[4],
            'type': g[3],
            'partial_condition': g[6] or '',
            'unique': g[0] == 'UNIQUE',
            'pk': g[1].endswith('_pkey'),
            'indexdef': indexdef
        }
        return definition


@click.group()
@click.pass_context
def cli(ctx):
    ctx.obj = IndexManager(settings.JSEARCH_MAIN_DB)


@cli.command()
@click.pass_obj
def inspect(mgr):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mgr.inspect())


@cli.command()
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


@cli.command()
@click.option('--dry-run', is_flag=True, help='Dry run - just print DROP INDEX statements, not actually run them in DB')
@click.pass_obj
def drop_all(mgr, dry_run):
    click.echo('Indexes to drop:')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mgr.drop_all(dry_run))


@cli.command()
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


@cli.command()
@click.pass_obj
def vacuum_status(mgr):
    loop = asyncio.get_event_loop()
    status = loop.run_until_complete(mgr.get_vacuum_status())


def run():
    cli()


if __name__ == '__main__':
    run()
