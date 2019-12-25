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


def shutdown_root_worker(fut: asyncio.Future, service: mode.Service) -> None:
    """Handles whole `mode`'s services tree based on future's result.

    Main purpose of this callback is avoiding services being stuck upon
    `asyncio.CancelledError` propagated to an important service's task.

    E.g., `pg_cancel_backend(pid)` will propagate `QueryCanceledError` which
    will be turned into `asyncio.CancelledError` by `aiopg` and this will
    rightfully stop `mode.Service.Task` from execution but won't stop the
    service itself.

    This leads to an invalid services' state where the service is up and running
    but actually doesn't do anything.

    """
    try:
        fut.result()
        service._log_mundane('Main future has completed, stopping the service tree...')
    except asyncio.CancelledError:
        service._log_mundane('Main future has been cancelled, stopping the service tree...')
    except Exception as exc:
        service._log_mundane('Main future raised an Exception, crashing the service tree...')
        asyncio.create_task(service.crash(exc))
        return

    if hasattr(service.beacon.root.data, '_starting_fut'):
        service.beacon.root.data._starting_fut.cancel()
