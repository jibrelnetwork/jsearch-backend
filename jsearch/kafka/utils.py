import asyncio
from typing import Any, Dict, List, Union

import backoff

from jsearch.kafka.listeners.reply import get_reply_listener
from jsearch.kafka.msg import make_request, get_uuid
from jsearch.kafka.producer import get_producer, get_sync_producer


@backoff.on_exception(backoff.fibo, max_tries=5, exception=asyncio.TimeoutError)
async def ask(topic: str, value: Dict[str, Any]):
    producer = get_producer()
    listener = await get_reply_listener()

    msg = make_request(value, reply_to=listener.topic)
    await producer.send_and_wait(topic, msg)

    return await listener.get_reply(uuid=get_uuid(msg))


def sync_send(topic: str, value: Union[Dict[str, Any], List[Any]]):
    msg = make_request(value)

    producer = get_sync_producer()
    producer.send(topic=topic, msg=msg)
    producer.flush()
