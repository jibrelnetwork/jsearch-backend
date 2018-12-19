import logging

from jsearch.common import contracts
from jsearch.common.contracts import ERC20_ABI
from jsearch.typing import Log

logger = logging.getLogger(__name__)

TRANSFER_EVENT_INPUT_SIZE = 3


class EventTypes:
    TRANSFER = 'Transfer'


def decode_erc20_tx_log(log: Log) -> Log:
    try:
        event = contracts.decode_event(ERC20_ABI, log)
    except Exception as e:
        logger.debug(e)
        logger.debug('Log decode error: <%s>\n ', log)
    else:
        event_type = event.pop('_event_type')
        log.update({
            'event_type': event_type,
            'event_args': event
        })
    return log


def process_log_event(log: Log) -> Log:
    log = decode_erc20_tx_log(log)

    if log.get('event_type') is not None:
        event_type = log.get('event_type')
        event_args = log['event_args']

        if event_type == EventTypes.TRANSFER and len(event_args) == TRANSFER_EVENT_INPUT_SIZE:
            log['is_token_transfer'] = True

    if 'is_token_transfer' not in log:
        log.update({
            'is_token_transfer': False,
            'event_type': None,
            'event_args': None,
        })
    log['is_processed'] = True
    return log
