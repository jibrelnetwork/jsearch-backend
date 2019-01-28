import asyncio
from typing import Dict, Any
from uuid import uuid4

import async_lru
from aiokafka import AIOKafkaConsumer
from mode import Service

from jsearch.async_utils import timeout
from jsearch.kafka.consumer import get_consumer
from jsearch.kafka.logs import log
from jsearch.kafka.msg import read_reply


class ReplyListener(Service):
    cache: Dict[str, Any]

    consumer: AIOKafkaConsumer

    group: str = f"jsearch-replies"
    topic: str = f"f-reply-jsearch-{str(uuid4())}"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = {}
        self.consumer = get_consumer(self.group, self.topic)

        log.info('["SERVICE BUS] Start new listener for replies %s', self.topic)

    async def on_start(self):
        await self.consumer.start()

    async def on_stop(self):
        await self.consumer.stop()

    @timeout()
    async def get_reply(self, uuid: str):
        while True:
            if uuid in self.cache:
                return self.cache.pop(uuid)

            log.debug("[SERVICE BUS] Still waiting reply for %s", uuid)
            await asyncio.sleep(0.1)

    @Service.task
    async def listen_replies(self):
        async for msg in self.consumer:
            log.debug("[SERVICE BUS] Get replies by key %s with value %s", msg.key, msg.value)
            uuid, value = read_reply(msg.value)
            self.cache[uuid] = value


@async_lru.alru_cache()
async def get_reply_listener() -> ReplyListener:
    listener = ReplyListener()
    await listener.start()
    return listener
