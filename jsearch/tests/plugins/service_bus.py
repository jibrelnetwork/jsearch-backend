import asyncio
from collections import defaultdict

import pytest
from asynctest import CoroutineMock
from itertools import chain
from typing import Any, DefaultDict


class KafkaMessage:

    def __init__(self, key=None, value=None, topic=None):
        self.key = key
        self.topic = topic
        self.value = value


@pytest.fixture()
def kafka_buffer() -> DefaultDict[str, Any]:
    return defaultdict(list)


@pytest.fixture()
def get_erc20_transfers_from_kafka(kafka_buffer):
    from jsearch.service_bus import ROUTE_HANDLE_ERC20_TRANSFERS

    def _wrapper():
        return list(chain(*kafka_buffer[ROUTE_HANDLE_ERC20_TRANSFERS]))

    return _wrapper


@pytest.fixture()
def mock_service_bus(mocker, kafka_buffer):
    mocker.patch('jsearch.service_bus.service_bus.start', CoroutineMock())
    mocker.patch('jsearch.service_bus.service_bus.stop', CoroutineMock())

    async def send_to_stream(route, value):
        kafka_buffer[route].insert(0, value)
        return asyncio.sleep(0)

    mocker.patch('jsearch.service_bus.service_bus.send_to_stream', send_to_stream)


@pytest.fixture()
def mock_service_bus_sync_client(mocker, kafka_buffer):
    mocker.patch('jsearch.service_bus.sync_client.start')
    mocker.patch('jsearch.service_bus.sync_client.stop')

    def send_to_stream(route, value):
        kafka_buffer[route].insert(0, value)

    mocker.patch('jsearch.service_bus.sync_client.send_to_stream', send_to_stream)
