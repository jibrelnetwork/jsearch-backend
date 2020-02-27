import logging
import time
from typing import Any

from jsearch.common import worker
from jsearch.common.structs import BlockRange
from jsearch.syncer import services
from jsearch.syncer.pool import WorkersPool
from jsearch.syncer.state import SyncerState

logger = logging.getLogger("syncer")


def run_worker(
        sync_range: BlockRange,
        api_port: int,
        resync: bool,
        resync_chain_splits: bool
) -> None:
    syncer_state = SyncerState(started_at=int(time.time()))
    api_worker = services.ApiService(port=api_port, state=syncer_state)

    syncer = services.SyncerService(
        sync_range=sync_range,
        resync=resync,
        resync_chain_splits=resync_chain_splits,
        state=syncer_state,
    )
    syncer.add_dependency(api_worker)

    worker.Worker(syncer).execute_from_commandline()


def run_workers_pool(sync_range: BlockRange, workers: int, **kwargs: Any) -> None:
    pool = WorkersPool(
        sync_range=sync_range,
        workers=workers,
        worker_kwargs=kwargs
    )

    api_service = services.ScalerWebService(pool=pool)
    pool_service = services.WorkersPoolService(pool=pool)

    api_service.add_dependency(pool_service)
    worker.Worker(api_service).execute_from_commandline()
