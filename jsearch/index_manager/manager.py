import asyncio
import logging
import os

import aiopg
import re
import yaml
from psycopg2.extras import DictCursor

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
            stmt = """CREATE {unique} INDEX {concurrently}\
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

        act_q = """SELECT now() - query_start, state, wait_event
                       FROM pg_stat_activity WHERE (query like %s OR query like %s) AND state <> 'idle'"""
        term = 'CREATE % INDEX % {} %'.format(self.name)
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
                        'size': index_size or '0 bytes'}

        stat_q = """SELECT * from pg_stat_user_indexes WHERE indexrelname=%s"""
        async with conn.cursor(cursor_factory=DictCursor) as cur:
            await cur.execute(stat_q, [self.name])
            res = await cur.fetchone()
            if res is not None:
                return {'name': self.name, 'status': 'EXISTS', 'size': index_size}

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
                try:
                    await cur.execute(cmd)
                except Exception as e:
                    logger.error(e)
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
        conn = await self.connect()
        if tables:
            indexes = [i for i in self.indexes if i.table in tables]
        else:
            indexes = self.indexes
        for idx in indexes:
            stmt = idx.get_create_statement(concurrently)
            if dry_run is True:
                logger.info('/* dry run */ %s', stmt)
            status = await idx.get_status(conn)
            if status['status'] == 'NOT EXISTS':
                self.queue.append(stmt)
            else:
                logger.info('Skipping %s [%s]', idx.name, status['status'])
        if dry_run is True:
            return
        conn.close()
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

            counter_q = """
            WITH max_age AS (
                SELECT 2000000000 as max_old_xid
                    , setting AS autovacuum_freeze_max_age
                    FROM pg_catalog.pg_settings
                    WHERE name = 'autovacuum_freeze_max_age' ), per_database_stats AS (
                SELECT datname
                    , m.max_old_xid::int
                    , m.autovacuum_freeze_max_age::int
                    , age(d.datfrozenxid) AS oldest_current_xid
                FROM pg_catalog.pg_database d
                JOIN max_age m ON (true)
                WHERE d.datallowconn )
            SELECT max(oldest_current_xid) AS oldest_current_xid,
                    max(ROUND(100*(oldest_current_xid/max_old_xid::float))) AS percent_towards_wraparound,
                    max(ROUND(100*(oldest_current_xid/autovacuum_freeze_max_age::float)))
                        AS percent_towards_emergency_autovac
            FROM per_database_stats;
            """
            async with conn.cursor(cursor_factory=DictCursor) as cur:
                await cur.execute(counter_q)
                counter_result = await cur.fetchone()

            return {'progress': progress_result,
                    'history': history_result,
                    'counter': counter_result}
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
