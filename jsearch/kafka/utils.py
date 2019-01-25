from typing import Any, Dict

from aiokafka import AIOKafkaProducer

from jsearch.kafka.listeners.reply import get_reply_listener
from jsearch.kafka.msg import make_request, get_uuid


async def ask(topic: str, value: Dict[str, Any], producer: AIOKafkaProducer):
    listener = await get_reply_listener()

    msg = make_request(value, reply_to=listener.topic)
    await producer.send_and_wait(topic, msg)

    return await listener.get_reply(uuid=get_uuid(msg))
