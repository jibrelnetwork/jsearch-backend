import abc
from typing import Any, Dict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from mode import Service

from jsearch.kafka.consumer import get_consumer
from jsearch.kafka.logs import log
from jsearch.kafka.msg import read_request, make_reply
from jsearch.kafka.producer import get_producer


class RequestListener(Service):
    group: str
    topic: str

    consumer: AIOKafkaConsumer
    producers: AIOKafkaProducer

    def __init__(self, group, topic, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.group = group
        self.topic = topic

        self.consumer = get_consumer(self.group, self.topic)
        self.producer = get_producer()

    async def on_start(self):
        await self.consumer.start()
        await self.producer.start()

    async def on_stop(self):
        await self.consumer.stop()
        await self.producer.stop()

    @Service.task
    async def listen_requests(self):
        async for msg in self.consumer:
            try:
                log.debug("[KAFKA] Get requests by key %s with value %s", msg.key, msg.value)

                uuid, topic, request = read_request(msg.value)
                value = await self.handle_request(request)
                reply = make_reply(uuid, value)

                await self.producer.send(topic, reply)

            except ValueError:
                log.exception('[KAFKA] Error when process message: %s, %s', msg.key, msg.value)
                continue

    @abc.abstractmethod
    async def handle_request(self, msg) -> Dict[str, Any]:
        pass
