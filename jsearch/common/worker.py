import mode


class NoLoggingOverrideWorker(mode.Worker):
    """Patched `Worker` with disabled `_setup_logging`.

    Default `mode.Worker` overrides logging configuration. This hack is there to
    deny this behavior.

    """
    def _setup_logging(self) -> None:
        pass
