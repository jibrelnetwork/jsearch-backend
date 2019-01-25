import asyncio
from typing import Dict, Any
from uuid import uuid4

import async_lru
import async_timeout
from aiokafka import AIOKafkaConsumer
from mode import Service

from jsearch.kafka.consumer import get_consumer
from jsearch.kafka.logs import log
from jsearch.kafka.msg import read_reply

DEFAULT_TIMEOUT = 60 * 5  # sec


class ReplyListener(Service):
    cache: Dict[str, Any]

    topic: str
    consumer: AIOKafkaConsumer

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = {}

        self.group = f"jsearch-replies"
        self.topic = f"f-reply-{str(uuid4())}"

        self.consumer = get_consumer(self.group, self.topic)

    async def on_start(self):
        await self.consumer.start()

    async def on_stop(self):
        await self.consumer.stop()

    async def get_reply(self, uuid: str, timeout=DEFAULT_TIMEOUT):
        async with async_timeout.timeout(timeout):
            while True:
                if uuid in self.cache:
                    return self.cache[uuid]

                await asyncio.sleep(0.1)

    @Service.task
    async def listen_replies(self):
        async for msg in self.consumer:
            log.debug("[KAFKA] Get replies by key %s with value %s", msg.key, msg.value)

            uuid, value = read_reply(msg.value)
            self.cache[uuid] = value


@async_lru.alru_cache()
async def get_reply_listener() -> ReplyListener:
    listener = ReplyListener()
    await listener.start()
    return listener
