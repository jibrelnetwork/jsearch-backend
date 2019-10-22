from dataclasses import dataclass

import time
from datetime import datetime
from typing import Optional

from jsearch.common.structs import BlockRange


@dataclass
class SyncerState:
    last_processed_block: int = 0

    last_check: float = 0.0
    started_at: int = 0

    total_blocks: int = 0
    last_check_blocks: int = 0
    new_check_blocks: int = 0

    hole: Optional[BlockRange] = None

    already_processed: Optional[int] = None
    checked_on_holes: Optional[BlockRange] = None

    CHECK_TIMEOUT: int = 30

    def as_dict(self):
        return {
            'last_block': self.last_processed_block,
            'speed': self.total_speed,
            'speed_last_30_seconds': self.speed,
            'started_at': datetime.fromtimestamp(self.started_at).isoformat(),
            'blocks': self.total_blocks,
            'hole': self.hole,
        }

    def update(self, current_block: int):
        self.last_processed_block = current_block

        self.total_blocks += 1
        self.new_check_blocks += 1

        if not self.last_check:
            self.last_check = time.monotonic()

        if (time.monotonic() - self.last_check) > self.CHECK_TIMEOUT:
            self.last_check = time.monotonic()
            self.last_check_blocks = self.new_check_blocks
            self.new_check_blocks = 0

    @property
    def speed(self):
        if self.last_check_blocks:
            return round(self.last_check_blocks / self.CHECK_TIMEOUT, 3)
        return 0

    @property
    def total_speed(self):
        if self.total_blocks:
            return round(self.total_blocks / (time.time() - self.started_at))
