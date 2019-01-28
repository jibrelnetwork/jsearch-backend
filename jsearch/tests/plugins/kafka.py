import asyncio

import async_timeout
import pytest
from asynctest import CoroutineMock


class KafkaMessage:

    def __init__(self, key=None, value=None, topic=None):
        self.key = key
        self.topic = topic
        self.value = value


@pytest.fixture()
def kafka_buffer():
    return []


@pytest.fixture()
def mock_reply_listener(mocker, kafka_buffer):
    mocker.patch('jsearch.kafka.consumer.AIOKafkaConsumer.start', CoroutineMock())
    mocker.patch('jsearch.kafka.consumer.AIOKafkaConsumer.stop', CoroutineMock())

    from jsearch.kafka.listeners.reply import get_reply_listener

    get_reply_listener.cache_clear()

    async def next_message(*args, **kwargs):
        async with async_timeout.timeout(10):
            while True:
                if kafka_buffer:
                    break
                await asyncio.sleep(1)

        return KafkaMessage(value=kafka_buffer.pop())

    mocker.patch('jsearch.kafka.consumer.AIOKafkaConsumer.__anext__', next_message)
    from jsearch.kafka.listeners.reply import get_reply_listener
    get_reply_listener.cache_clear()


@pytest.fixture()
def mock_request_listener(mocker, kafka_buffer):
    mocker.patch('jsearch.kafka.producer.AIOKafkaProducer.start', CoroutineMock())
    mocker.patch('jsearch.kafka.producer.AIOKafkaProducer.stop', CoroutineMock())

    async def send_and_wait(obj, topic, msg):
        kafka_buffer.append(msg)
        await asyncio.sleep(0)

    mocker.patch('jsearch.kafka.producer.AIOKafkaProducer.send_and_wait', send_and_wait)

    from jsearch.kafka.producer import get_producer
    get_producer.cache_clear()


@pytest.fixture()
def mock_kafka(mock_reply_listener, mock_request_listener):
    pass
