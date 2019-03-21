from uuid import uuid4

import pytest
from asynctest import CoroutineMock, Mock
from kafka import TopicPartition
from kafka.structs import KafkaMessage


@pytest.fixture()
def mock_last_block_consumer(mocker):
    from jsearch.service_bus import ROUTE_HANDLE_LAST_BLOCK

    def _wrapper(value):
        consumer_mock = Mock()
        consumer_mock.start = CoroutineMock()
        consumer_mock.stop = CoroutineMock()
        consumer_mock.end_offsets = CoroutineMock(return_value={TopicPartition(ROUTE_HANDLE_LAST_BLOCK, 0): 1})
        consumer_mock.getone = CoroutineMock(
            return_value=KafkaMessage(
                topic=ROUTE_HANDLE_LAST_BLOCK,
                partition=0,
                offset=1,
                key=str(uuid4()),
                value={"value": value},
            )
        )

        mock = Mock(return_value=consumer_mock)

        mocker.patch('jsearch.common.last_block._get_consumer', mock)

    return _wrapper
