import logging
from typing import NamedTuple, List, Optional

from jsearch import settings
from jsearch.common import contracts
from jsearch.common.contracts import DEX_ABI
from jsearch.common.wallet_events import make_event_index_for_log
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


ORDER_STATUSES = {
    'Placed': DexEventType.ORDER_PLACED,
    'Activated': DexEventType.ORDER_ACTIVATED,
    'Completed': DexEventType.ORDER_COMPLETED,
    'Cancelled': DexEventType.ORDER_CANCELLED,
    'Expired': DexEventType.ORDER_EXPIRED,
}
ORDER_EVENT_TYPE_TO_STATUS = {value: key for key, value in ORDER_STATUSES.items()}


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
        event = contracts.decode_event(DEX_ABI, log)
    except Exception:  # NOQA: Logged by 'exc_info'.
        logger.debug('Log decode error', extra={'log': log})
        return None

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
                'event_index': make_event_index_for_log(
                    log['block_number'],
                    log['transaction_index'],
                    log['log_index']
                ),
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
