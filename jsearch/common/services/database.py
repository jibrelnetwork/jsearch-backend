import backoff
import mode
import psycopg2

from aiopg import sa


class DatabaseService(mode.Service):
    """The `mode.Service` wrapper for database access.

    Usage:

        database = DatabaseService(dsn=settings.DESIRED_DB_DSN)

        def something_doer():
            async with database.engine.acquire() as conn:
                ...

        if __name__ == '__main__':
            worker.Worker(
                database,
               ...
            ).execute_from_command_line()

    """
    engine: sa.Engine

    def __init__(self, dsn, **kwargs):
        self.dsn = dsn

        super().__init__(**kwargs)

    @backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
    async def on_start(self) -> None:
        self.engine = await sa.create_engine(self.dsn)

    async def on_stop(self) -> None:
        if self.engine is None:
            return

        self.engine.terminate()
