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


def get_order_id(topics: List[str], event_type) -> Optional[str]:
    """
    event OrderPlacedEvent(
       address indexed orderCreator,
       string indexed orderID,
       OrderType orderType,
       address indexed tradedAsset,
       uint256 tradedAmount,
       address fiatAsset,
       uint256 assetPrice,
       uint256 expirationTimestamp
    );
    event OrderActivatedEvent(string indexed orderID);
    event OrderCompletedEvent(string indexed orderID);
    event OrderCancelledEvent(string indexed orderID);
    event OrderExpiredEvent(string indexed orderID);

    event TradePlacedEvent(
      address indexed tradeCreator,
      uint256 indexed tradeID,
      string indexed orderID,
      uint256 tradedAmount
    );

    Note:
        This is ugly workaround.

        Indexed string stores as hash in a log topics
        Syncer cann't parse such value.
    """
    if event_type == DexEventType.ORDER_PLACED:
        order_id = topics[2]
    elif event_type in (DexEventType.ORDER_ACTIVATED,
                        DexEventType.ORDER_CANCELLED,
                        DexEventType.ORDER_COMPLETED,
                        DexEventType.ORDER_EXPIRED):
        order_id = topics[1]
    elif event_type == DexEventType.TRADE_PLACED:
        order_id = topics[3]
    else:
        order_id = None

    return order_id


def decode_dex_tx_log(log: Log) -> Optional[AnyDict]:
    try:
        event = contracts.decode_event(DEX_ABI, log)
    except Exception:  # NOQA: Logged by 'exc_info'.
        logger.debug('Log decode error', extra={'log': log})
    else:
        event_type = event.pop('_event_type')
        order_id = get_order_id(log['topics'], event_type)
        if order_id is not None:
            event['orderID'] = order_id

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
