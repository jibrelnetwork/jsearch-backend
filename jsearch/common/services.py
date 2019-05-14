from asyncio import AbstractEventLoop
from typing import Any, Callable

import mode
from aiohttp import web

AppMaker = Callable[[AbstractEventLoop], web.Application]


class ApiService(mode.Service):
    def __init__(self, port: int, app_maker: AppMaker, *args: Any, **kwargs: Any) -> None:
        self.port = port

        super(ApiService, self).__init__(*args, **kwargs)

        self.app = app_maker(self.loop)
        self.runner = web.AppRunner(self.app)

    async def on_start(self) -> None:
        await self.runner.setup()
        await web.TCPSite(self.runner, '0.0.0.0', self.port).start()

    async def on_stop(self) -> None:
        await self.runner.cleanup()
