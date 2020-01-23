import mode

from jsearch.syncer.database import MainDB


class DBService(mode.Service):
    main_db: MainDB

    def __init__(self, main_db, *args, **kwargs):
        self.main_db = main_db
        super(DBService, self).__init__(*args, **kwargs)

    async def on_start(self) -> None:
        await self.main_db.connect()

    async def on_stop(self) -> None:
        await self.main_db.disconnect()
