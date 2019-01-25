import asyncio
import json
from typing import Any, Dict

from aiokafka import AIOKafkaProducer

from jsearch import settings


def serializer(value: Dict[str, Any]) -> str:
    return json.dumps(value).encode()


def get_producer():
    loop = asyncio.get_event_loop()
    return AIOKafkaProducer(
        loop=loop,
        bootstrap_servers=settings.KAFKA,
        value_serializer=serializer
    )
