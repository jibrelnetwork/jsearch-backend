from typing import Any, Callable
import logging

import mode
from aiohttp import web


logger = logging.getLogger(__name__)
AppMaker = Callable[[], web.Application]


class ApiService(mode.Service):
    def __init__(self, port: int, app_maker: AppMaker, *args: Any, **kwargs: Any) -> None:
        self.port = port

        # FIXME (nickgashkov): `mode.Service` does not support `*args`
        super(ApiService, self).__init__(*args, **kwargs)  # type: ignore

        self.app = app_maker()
        self.runner = web.AppRunner(self.app)

    async def on_start(self) -> None:
        await self.runner.setup()
        logger.info("Starting Syncer API at port %s", self.port)
        await web.TCPSite(self.runner, '0.0.0.0', self.port).start()

    async def on_stop(self) -> None:
        await self.runner.cleanup()

    async def stop(self):
        await self.on_stop()
