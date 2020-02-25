from typing import Callable, Any

import pytest

from jsearch.common.processing.dex_logs import DexEventType


@pytest.mark.parametrize("event_type", DexEventType.ALL)
def test_log_to_dex_event(
        event_type: str,
        dex_log_dict_factory: Callable[..., Any]
):
    from jsearch.common.processing.dex_logs import logs_to_dex_events
    log = dex_log_dict_factory(event_type=event_type)

    # when
    events = logs_to_dex_events([log])

    assert len(events) == 1, 'Event is not parsed correctly'

    # then
    for event in events:
        assert event['timestamp'] == log['timestamp']
        assert event['is_forked'] == log['is_forked']
        assert event['block_hash'] == log['block_hash']
        assert event['block_number'] == log['block_number']
        assert event['event_type'] == event_type
