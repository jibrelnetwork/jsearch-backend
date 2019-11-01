
import asyncio
import logging

import aiopg

from jsearch import settings


logger = logging.getLogger('index_manager')


class Index:
    name: str
    definition: str
    pk: bool
    table: str

    def __init__(self, name, table, definition, pk=False):
        self.name = name
        self.table = table
        self.definition = definition
        self.pk = pk

    def get_drop_statement(self):
        if self.pk is False:
            stmt = """DROP INDEX {}""".format(self.name)
        else:
            stmt = """ALTER TABLE {} DROP CONSTRAINT {}""".format(self.table, self.name)
        return stmt

    def get_create_statement(self):
        if self.pk is False:
            stmt = """CREATE INDEX IF NOT EXISTS {} ON {} {}""".format(self.name, self.table, self.definition)
        else:
            stmt = """ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY {};""".format(self.table, self.name, self.definition)
        return stmt

    async def get_status(self, conn):
        size_q = """SELECT nspname || '.' || relname AS "relation",
                            pg_size_pretty(pg_relation_size(C.oid)) AS "size"
                    FROM pg_class C
                    LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
                    WHERE nspname = 'public' AND relname=%s;"""
        async with conn.cursor() as cur:
            await cur.execute(size_q, self.name)
            res = await cur.fetchone()
            if res is None:
                return {'status': 'NOT EXISTS'}
            else:
                index_size = res['size']

        stat_q = """SELECT * from pg_stat_user_indexes WHERE indexrelname=%s"""
        async with conn.cursor() as cur:
            await cur.execute(stat_q, self.name)
            res = await cur.fetchone()
            if res is not None:
                return {'status': 'EXISTS', 'size': index_size}

        act_q = """SELECT now() - query_start, state, wait_event FROM pg_stat_activity WHERE query=%s AND state <> 'idle'"""
        async with conn.cursor() as cur:
            await cur.execute(act_q, self.get_create_statement())
            res = await cur.fetchone()
            if res is not None:
                return {'status': 'CREATING',
                        'query_duration': res[0],
                        'query_state': res[1],
                        'query_wait': res[2],
                        'current_size': index_size}
        assert False, 'Index Status miss'


class IndexManager:

    def __init__(self, db_connection_string):
        self.db_connection_string=db_connection_string

    async def connect(self):
        conn = await aiopg.connect(settings.JSEARCH_MAIN_DB, timeout=None)
        return conn

    async def drop_indexes(self):
        pass

    async def create_indexes(self):
        pass

    async def get_indexes_status(self):
        conn = await self.connect()
        pass

    async def inspect(self):
        conn = await self.connect()
        q = """SELECT * FROM pg_indexes WHERE schemaname='public';"""
        async with conn.cursor() as cur:
            await cur.execute(q)
            res = await cur.fetchall()
        for row in res:

            print('\nname=\'{}\''.format(row['indexname']))
            print('table=\'{}\''.format(row['tablename']))
            print('definition=\'{}\'\n'.format(row['indexdef']))




if __name__ == '__main__':
    