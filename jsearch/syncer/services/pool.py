import mode
from typing import Any

from jsearch.syncer.pool import WorkersPool
from jsearch.syncer.utils import get_last_block


class WorkersPoolService(mode.Service):
    _pool: WorkersPool

    def __init__(self, pool: WorkersPool, *args: Any, **kwargs: Any):
        self._pool = pool

        # FIXME (nickgashkov): `mode.Service` does not support `*args`
        super(WorkersPoolService, self).__init__(*args, **kwargs)  # type: ignore

    async def on_start(self) -> None:
        last_block = get_last_block()
        await self._pool.run(last_block)

    async def on_stop(self) -> None:
        await self._pool.terminate()

    @mode.Service.task
    async def pool(self):
        try:
            await self._pool.wait()
        finally:
            await self.stop()
