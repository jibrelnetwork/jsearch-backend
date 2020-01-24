import asyncio
import functools
import logging
from typing import Any

import mode

from jsearch.common import stats
from jsearch.common.prom_metrics import METRIC_SYNCER_CHAIN_CONSISTENCY
from jsearch.common.worker import shutdown_root_worker
from jsearch.monitor.db import MainDB

logger = logging.getLogger(__name__)

TIMEOUT = 60


class ConsistencyCollector(mode.Service):
    main_db: MainDB

    def __init__(self, main_db: MainDB, *args: Any, **kwargs: Any) -> None:
        self.main_db = main_db
        super(ConsistencyCollector, self).__init__(*args, **kwargs)  # type: ignore

    async def on_started(self) -> None:
        task = asyncio.create_task(self.update_consistency_statistics())
        task.add_done_callback(functools.partial(shutdown_root_worker, service=self))

    async def update_consistency_statistics(self) -> None:
        while not self.should_stop:
            chain_stats = await stats.get_chain_stats(engine=self.main_db.engine)

            holes_count = len(chain_stats.chain_holes)
            METRIC_SYNCER_CHAIN_CONSISTENCY.set(holes_count)
            await asyncio.sleep(60)
