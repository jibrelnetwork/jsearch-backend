import logging
from typing import List
from typing import Tuple, Dict, Any

from jsearch.common import contracts
from jsearch.common.contracts import ERC20_ABI
from jsearch.typing import Log, EventArgs, Abi

logger = logging.getLogger(__name__)

TRANSFER_EVENT_INPUT_SIZE = 3


class EventTypes:
    TRANSFER = 'Transfer'


def decode_erc20_tx_log(log: Log) -> Log:
    try:
        event = contracts.decode_event(ERC20_ABI, log)
    except Exception:  # NOQA: Logged by 'exc_info'.
        logger.debug('Log decode error', extra={'log': log}, exc_info=True)
    else:
        event_type = event.pop('_event_type')
        log.update({
            'event_type': event_type,
            'event_args': event
        })
    return log


def get_event_inputs_from_abi(abi: Abi) -> List[Dict[str, Any]]:
    """
    Args:
        abi: contract abi

    Returns:
        only event inputs

    Notes:
        some contracts (for example 0xaae81c0194d6459f320b70ca0cedf88e11a242ce) may have
        several Transfer events with different signatures,
        so we try to find ERS20 copilent event (with 3 args)

    """
    for interface in abi:
        is_it_event_input = interface['type'] == 'event'
        is_it_event_input = is_it_event_input and interface.get('name') == EventTypes.TRANSFER
        is_it_event_input = is_it_event_input and len(interface['inputs']) == TRANSFER_EVENT_INPUT_SIZE

        if is_it_event_input:
            return interface['inputs']

    raise ValueError('No inputs')


def get_transfer_details_from_erc20_event_args(
        event_args: EventArgs,
        abi: Abi,
) -> Tuple[str, str, str]:
    """
    Eject transfer details from event
    """
    event_inputs = get_event_inputs_from_abi(abi)

    args_keys = [interface_type['name'] for interface_type in event_inputs]
    args_values = [event_args[key] for key in args_keys]

    from_address = args_values[0]
    to_address = args_values[1]
    token_amount = args_values[2]

    return from_address, to_address, token_amount


def process_log_event(log: Log) -> Log:
    log = decode_erc20_tx_log(log)

    if log.get('event_type') is not None:
        event_type = log.get('event_type')
        event_args = log['event_args']

        if event_type == EventTypes.TRANSFER and len(event_args) == TRANSFER_EVENT_INPUT_SIZE:
            from_address, to_address, token_amount = get_transfer_details_from_erc20_event_args(
                event_args=event_args, abi=ERC20_ABI
            )
            log.update({
                'is_token_transfer': True,
                'token_transfer_to': to_address,
                'token_transfer_from': from_address,
                'token_amount': token_amount,
            })
    return log
