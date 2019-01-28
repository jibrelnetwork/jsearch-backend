pytest_plugins = (
    'jsearch.tests.plugins.kafka',
)


async def test_ask(mocker, mock_kafka):
    mocker.patch('jsearch.async_utils.DEFAULT_TIMEOUT', 5)

    # given - test message
    from jsearch.kafka.utils import ask
    # when - ask service to reply
    reply = await ask(topic='request', value={'request': 'test'})

    # then - check reply
    assert reply == {"request": "test"}
