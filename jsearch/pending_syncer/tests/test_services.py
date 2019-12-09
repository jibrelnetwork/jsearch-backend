import asyncio

from contextlib import suppress
from typing import Coroutine

import pytest

from click.testing import CliRunner
from mode import Worker, Service
from pytest_mock import MockFixture

from jsearch.pending_syncer.main import run
from jsearch.pending_syncer.services.syncer import PendingSyncerService


@pytest.fixture(autouse=True)
def patch_worker_shutdown_loop(mocker: MockFixture) -> None:
    """Patches worker's shutdown to gather all futures but leave loop open."""

    def _shutdown_loop(self) -> None:
        with suppress(asyncio.CancelledError):
            self.loop.run_until_complete(self._gather_futures())

        self._gather_all()
        self.loop.run_until_complete(asyncio.ensure_future(self._sentinel_task(), loop=self.loop))

        if self.crash_reason:
            raise self.crash_reason from self.crash_reason

    mocker.patch.object(Worker, '_shutdown_loop', _shutdown_loop)


@pytest.fixture(autouse=True)
def reset_event_loop() -> None:
    """Sets the new event loop, so `mode.Worker` can use `loop.run_forever`."""
    asyncio.set_event_loop(asyncio.new_event_loop())


async def cancel(self: Service) -> None:
    raise asyncio.CancelledError()


async def noop(self: Service) -> None:
    pass


async def fail(self: Service) -> None:
    1 / 0


@pytest.mark.parametrize(
    'task, expected_exit_code',
    (
        (cancel, 0),
        (noop, 0),
        (fail, 1),
    ),
    ids=(
        'syncer is cancelled -> worker exits with 0',
        'syncer has ended work -> worker exits with 0',
        'syncer fails -> worker exits with 1',
    ),
)
def test_worker_does_not_stuck(
        mocker: MockFixture,
        cli_runner: CliRunner,
        task: Coroutine,
        expected_exit_code: int,
) -> None:
    mocker.patch.object(PendingSyncerService, 'syncer', task)

    proc = cli_runner.invoke(run)
    proc_exit_code = proc.exit_code

    assert proc_exit_code == expected_exit_code, proc.stdout


def test_pending_syncer_actually_starts(
        mocker: MockFixture,
        cli_runner: CliRunner,
) -> None:
    started = []

    async def main(self: Service) -> None:
        started.append(self.__class__)
        raise asyncio.CancelledError()

    mocker.patch.object(PendingSyncerService, 'syncer', main)

    proc = cli_runner.invoke(run)
    proc_exit_code = proc.exit_code

    assert proc_exit_code == 0, proc.stdout
    assert started == [PendingSyncerService]


@pytest.mark.skip('TODO')
def test_worker_can_be_stopped_with_signals(
        mocker: MockFixture,
        cli_runner: CliRunner,
) -> None:
    pass
