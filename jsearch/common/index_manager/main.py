
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

INDEX_RE = 'CREATE\s?(UNIQUE)? INDEX (\w+) ON public.(\w+) USING (\w+) (\([^\)]*\))\s?(WHERE)?\s?(\([^\)]*\))?'
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

    def get_create_statement(self):
        if self.pk is False:
            options = {
                'name': self.name,
                'table': self.table,
                'fields': self.fields,
                'type': self.type,
                'unique': 'UNIQUE' if self.unique else '',
                'where': 'WHERE {}'.format(self.partial_condition) if self.partial_condition else '',
            }
            stmt = """CREATE {unique} INDEX IF NOT EXISTS {name} ON {table} USING {type} {fields} {where}""".format(
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
                return {'name': self.name, 'status': 'NOT EXISTS', 'size': '0 bytes'}
            else:
                index_size = res['size']

        stat_q = """SELECT * from pg_stat_user_indexes WHERE indexrelname=%s"""
        async with conn.cursor(cursor_factory=DictCursor) as cur:
            await cur.execute(stat_q, [self.name])
            res = await cur.fetchone()
            if res is not None:
                return {'name': self.name, 'status': 'EXISTS', 'size': index_size}

        act_q = """SELECT now() - query_start, state, wait_event FROM pg_stat_activity WHERE query=%s AND state <> 'idle'"""
        async with conn.cursor(cursor_factory=DictCursor) as cur:
            await cur.execute(act_q, [self.get_create_statement()])
            res = await cur.fetchone()
            if res is not None:
                return {'name': self.name,
                        'status': 'CREATING',
                        'query_duration': res[0],
                        'query_state': res[1],
                        'query_wait': res[2],
                        'current_size': index_size}
        assert False, 'Index Status miss'


class IndexManager:

    indexes = None

    def __init__(self, db_connection_string):
        self.db_connection_string=db_connection_string
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
        print('Worker {} STARTED'.format(n))
        while True:
            try:
                cmd = self.queue.pop()
            except IndexError:
                print('Worker {} WORK FINISHED'.format(n))
                break
            async with conn.cursor() as cur:
                print('Worker {} EXECUTE: {} '.format(n, cmd))
                await cur.execute(cmd)
        conn.close()

    async def drop_all(self):
        conn = await self.connect()
        for idx in self.indexes:
            stmt = idx.get_drop_statement()
            async with conn.cursor() as cur:
                await cur.execute(stmt)
            print(stmt)
        conn.close()

    async def create_all(self, workers_number):
        for idx in self.indexes:
            stmt = idx.get_create_statement()
            self.queue.append(stmt)
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
            print(yaml.dump(indexes, default_flow_style=False, sort_keys=False, line_break='\n\n'))
        finally:
            conn.close()


def parse_indexdef(indexdef):
    """
    (None, 'ix_logs_keyset_by_timestamp', 'logs', 'btree', '(address, "timestamp", transaction_index, log_index)', 'WHERE', '(is_forked = false)')
    :param indexdef:
    :return:
    """
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


@cli.command()
@click.pass_obj
def drop_all(mgr):
    click.echo('Dropping DB indexes:')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mgr.drop_all())


@cli.command()
@click.option('--workers', '-w', default=2, help='Number of parallel workers - create your indexes faster!!!*')
@click.pass_obj
def create_all(mgr, workers):
    click.echo('Creating DB indexes ({} workers):'.format(workers))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mgr.create_all(workers))


if __name__ == '__main__':
    cli()
