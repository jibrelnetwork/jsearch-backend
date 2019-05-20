import mode

from jsearch import utils


class Worker(mode.Worker):
    """Patched `Worker` with disabled `_setup_logging` and graceful shutdown.
    Default `mode.Worker` overrides logging configuration. This hack is there to
    deny this behavior.
    """
    async def on_shutdown(self) -> None:
        await utils.shutdown()

    def _setup_logging(self) -> None:
        pass
