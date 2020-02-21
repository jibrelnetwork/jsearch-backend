import logging
from typing import NamedTuple, List, Optional

from jsearch import settings
from jsearch.common import contracts
from jsearch.common.contracts import DEX_ABI
from jsearch.typing import Logs, AnyDict, Log

logger = logging.getLogger(__name__)


class DexEventType:
    ORDER_PLACED = 'OrderPlacedEvent'
    ORDER_ACTIVATED = 'OrderActivatedEvent'
    ORDER_COMPLETED = 'OrderCompletedEvent'
    ORDER_CANCELLED = 'OrderCancelledEvent'
    ORDER_EXPIRED = 'OrderExpiredEvent'

    TRADE_PLACED = 'TradePlacedEvent'
    TRADE_COMPLETED = 'TradeCompletedEvent'
    TRADE_CANCELLED = 'TradeCancelledEvent'

    TOKEN_BLOCKED = 'TokensBlockedEvent'
    TOKEN_UNBLOCKED = 'TokensUnblockedEvent'

    BLOCKS = (
        TOKEN_BLOCKED,
        TOKEN_UNBLOCKED
    )

    ORDERS = (
        ORDER_PLACED,
        ORDER_ACTIVATED,
        ORDER_COMPLETED,
        ORDER_CANCELLED,
        ORDER_EXPIRED,
    )

    TRADE = (
        TRADE_PLACED,
        TRADE_COMPLETED,
        TRADE_CANCELLED
    )

    ALL = (*ORDERS, *TRADE, *BLOCKS)


class DexEvent(NamedTuple):
    block_hash: str
    block_number: int

    tx_hash: str
    timestamp: int

    event_type: str
    is_forked: bool

    data: AnyDict


def decode_dex_tx_log(log: Log) -> Optional[AnyDict]:
    try:
        from pdb import set_trace; set_trace()
        event = contracts.decode_event(DEX_ABI, log)
    except Exception:  # NOQA: Logged by 'exc_info'.
        logger.exception('Log decode error', extra={'log': log})
    else:
        event_type = event.pop('_event_type')
        return {
            'event_type': event_type,
            'event_args': event
        }


def logs_to_dex_events(
        logs: Logs,
) -> List[AnyDict]:
    events = []
    for log in logs:
        contract = log['address'].lower()
        payload = log['event_args']
        event_type = log['event_type']
        if contract == settings.DEX_CONTRACT and event_type in DexEventType.ALL:
            event = {
                'tx_hash': log['transaction_hash'],
                'block_hash': log['block_hash'],
                'block_number': log['block_number'],
                'is_forked': log['is_forked'],
                'timestamp': log['timestamp'],
                'event_type': event_type,
                'event_data': payload,
            }
            events.append(event)

    return events


def process_dex_log(log: Log) -> Log:
    contract: str = log['address'].lower()
    if contract == settings.DEX_CONTRACT:
        update = decode_dex_tx_log(log)
        if update:
            log.update(update)
    return log
