import logging
from typing import Coroutine

from jsearch_service_bus.base import get_async_consumer
from kafka import TopicPartition

from jsearch import settings
from jsearch.service_bus import ROUTE_HANDLE_LAST_BLOCK
from jsearch.utils import Singleton

logger = logging.getLogger(__name__)


def _get_consumer():
    return get_async_consumer(
        group='last_block_loader',
        topic=ROUTE_HANDLE_LAST_BLOCK,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
    )


class LastBlock(Singleton):
    offset = settings.ETH_BALANCE_BLOCK_OFFSET

    number: int
    task: Coroutine[None, None, None]

    def __init__(self):
        self.number = None
        self.task = None

    @property
    def _partition(self):
        return TopicPartition(topic=ROUTE_HANDLE_LAST_BLOCK, partition=0)

    async def get(self):
        return self.number or await self.load()

    async def get_last_stable_block(self):
        return await self.get() - self.offset

    async def load(self):
        consumer = _get_consumer()

        offsets = await consumer.end_offsets(partitions=[self._partition])
        last_value_offset = offsets[ROUTE_HANDLE_LAST_BLOCK]

        await consumer.seek(self._partition, last_value_offset - 1)
        value = await consumer.getone(self._partition)

        await consumer.stop()

        number = value['number']
        self.update(number=number)

        return number

    def update(self, number):
        self.number = number
        logging.info("[LAST BLOCK] %s", self.number)
