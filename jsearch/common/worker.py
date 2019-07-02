import asyncio

import mode


class Worker(mode.Worker):
    """Patched `Worker` with disabled `_setup_logging` and graceful shutdown.
    Default `mode.Worker` overrides logging configuration. This hack is there to
    deny this behavior.
    """

    def _setup_logging(self) -> None:
        pass

    def schedule_shutdown(self):
        asyncio.ensure_future(self.stop())
