import asyncio
import logging
from typing import Coroutine, Union
from uuid import uuid4

from funcy import cached_property
from jsearch_service_bus.base import get_async_consumer
from kafka import TopicPartition

from jsearch import settings
from jsearch.service_bus import ROUTE_HANDLE_LAST_BLOCK
from jsearch.utils import Singleton

logger = logging.getLogger(__name__)


def _get_consumer():
    uuid = str(uuid4())
    return get_async_consumer(
        group=f'last_block_loader_{uuid}',
        topic=ROUTE_HANDLE_LAST_BLOCK,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
    )


class LastBlock(Singleton):
    offset = settings.ETH_BALANCE_BLOCK_OFFSET

    task: Coroutine[None, None, None]

    LATEST_BLOCK = 'latest'

    MODE_ALLOW_UPDATES = 'allow_updates'
    MODE_READ_ONLY = 'read_only'

    mode: str = MODE_ALLOW_UPDATES
    number: Union[int, str] = None

    @cached_property
    def _semaphore(self):
        return asyncio.Semaphore(1)

    @property
    def _partition(self):
        return TopicPartition(topic=ROUTE_HANDLE_LAST_BLOCK, partition=0)

    async def get(self):
        if self.mode == self.MODE_ALLOW_UPDATES:
            async with self._semaphore:
                if self.number is None:
                    await self._load()

        if self.number == self.LATEST_BLOCK:
            return self.number
        elif self.number:
            return self.number - self.offset
        return self.LATEST_BLOCK

    async def _load(self):
        logger.info('Loading last block from the topic...', extra={'tag': 'LAST BLOCK'})
        consumer = _get_consumer()
        await consumer.start()

        offsets = await consumer.end_offsets(partitions=[self._partition])

        last_value_offset = offsets[self._partition]
        if last_value_offset >= 1:
            last_value_offset -= 1

        consumer.seek(self._partition, last_value_offset)
        msg = await consumer.getone(self._partition)
        await consumer.stop()

        last_block = msg.value['value']['number']
        self.update(number=last_block)

        return last_block

    def update(self, number):
        if self.mode != self.MODE_ALLOW_UPDATES:
            return

        if self.number is None or self.number < number:
            self.number = number
            logger.info('Last block has been updated', extra={'tag': 'LAST BLOCK', 'number': number})
