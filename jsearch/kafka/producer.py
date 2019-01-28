import asyncio
import json
from functools import lru_cache
from typing import Any, Dict

from aiokafka import AIOKafkaProducer

from jsearch import settings


def serializer(value: Dict[str, Any]) -> str:
    return json.dumps(value).encode()


@lru_cache()
def get_producer():
    loop = asyncio.get_event_loop()
    return AIOKafkaProducer(
        loop=loop,
        bootstrap_servers=settings.KAFKA,
        value_serializer=serializer
    )
