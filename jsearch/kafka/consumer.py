import asyncio
import json
from functools import lru_cache
from typing import Any, Dict

from aiokafka import AIOKafkaConsumer

from jsearch import settings


def deserializer(serialized: str) -> Dict[str, Any]:
    return json.loads(serialized)


@lru_cache()
def get_consumer(group, *topics):
    loop = asyncio.get_event_loop()
    return AIOKafkaConsumer(
        *topics,
        loop=loop,
        bootstrap_servers=settings.KAFKA,
        group_id=group,
        value_deserializer=deserializer
    )
