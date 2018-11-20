import logging
from typing import Dict, Any, Optional, List, Tuple, Set

from jsearch.common import contracts
from jsearch.common.integrations.contracts import get_contract

logger = logging.getLogger(__name__)

TRANSFER_EVENT_INPUT_SIZE = 3


class EventTypes:
    TRANSFER = 'Transfer'


def get_event_inputs_from_abi(abi) -> Dict[Any, Any]:
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


def process_erc20_transfer_event(event: Dict[Any, Any], abi: Dict[Any, Any], token_decimals: int) \
        -> Tuple[str, str, str]:
    """
    Eject information about ERC20 transfer from event

    Args:
        event: event description
        abi: contract application binary interface as dict
        token_decimals: decimal offset for token amount

    Returns:
        from_address
        to_address
        token_amount
    """
    event_inputs = get_event_inputs_from_abi(abi)
    args_list = [event[interface_type['name']] for interface_type in event_inputs]

    from_address = args_list[0]
    to_address = args_list[1]
    token_amount = args_list[2] / (10 ** token_decimals)
    return from_address, to_address, token_amount


def get_contract_from_cache(address, cache) -> Optional[Dict]:
    if address not in cache:
        contract = get_contract(address=address)
        if not contract:
            return None

        cache[address] = contract

    return cache[address]


def process_logs(logs: List[Dict[Any, Any]], contract: Optional[Dict[Any, Any]] = None) \
        -> Tuple[List[Dict[Any, Any]], Set[Tuple[str, str]], Set[Tuple[str, str]]]:
    contracts_cache = {}
    need_update_token_info = set()
    need_update_token_balance = set()
    for log in logs:
        log['is_processed'] = True

        contract = contract or get_contract_from_cache(address=log['address'], cache=contracts_cache)
        if contract is None:
            log.update({
                'is_token_transfer': False,
                'token_transfer_to': None,
                'token_transfer_from': None,
                'token_amount': None,
                'event_type': None,
                'event_args': None,
            })
            continue

        abi = contract['abi']
        try:
            event = contracts.decode_event(abi, log)
        except Exception as e:
            logger.debug(e)
            logger.debug('Log decode error: <%s>\n ', log)
        else:
            event_type = event.pop('_event_type')
            log.update({
                'event_type': event_type,
                'event_args': event
            })

            token_decimals = contract['token_decimals']
            if token_decimals is None:
                need_update_token_info.add(contract['address'])

            if event_type == EventTypes.TRANSFER and len(event) == TRANSFER_EVENT_INPUT_SIZE:
                from_address, to_address, token_amount = process_erc20_transfer_event(event, abi, token_decimals)

                log.update({
                    'is_token_transfer': True,
                    'token_transfer_to': to_address,
                    'token_transfer_from': from_address,
                    'token_amount': token_amount,
                })

                need_update_token_balance.add((log['address'], from_address))
                need_update_token_balance.add((log['address'], to_address))

    return logs, need_update_token_info, need_update_token_balance
