import logging
import time
from collections import defaultdict
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Dict, Any, Optional, List, Tuple, Set

from jsearch import settings
from jsearch.common import contracts
from jsearch.common.integrations.contracts import get_contract
from jsearch.common.processing.operations import OPERATION_UPDATE_TOKEN_BALANCE, OPERATION_UPDATE_CONTRACT_INFO

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


def update_contract_cache(logs: List[Dict[str, Any]], contract_cache: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    log_contract_addresses = {log['address'] for log in logs}

    missed_addresses = log_contract_addresses - set(contract_cache.keys())
    if missed_addresses:

        tasks = []
        with ThreadPoolExecutor(settings.JSEARCH_POST_PROCESSING_THREADS) as executor:
            for address in missed_addresses:
                task = executor.submit(get_contract, address)
                tasks.append(task)

            for future in as_completed(tasks):
                contract = future.result()
                if contract:
                    address = contract['address']
                    contract_cache[address] = contract

    return contract_cache


def process_log(log: Dict[str, Any], contract: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    operations = defaultdict(set)
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
            operations[OPERATION_UPDATE_CONTRACT_INFO].add((contract['address'],))

        elif event_type == EventTypes.TRANSFER and len(event) == TRANSFER_EVENT_INPUT_SIZE:
            from_address, to_address, token_amount = process_erc20_transfer_event(event, abi, token_decimals)

            log.update({
                'is_token_transfer': True,
                'token_transfer_to': to_address,
                'token_transfer_from': from_address,
                'token_amount': token_amount,
            })

            operations[OPERATION_UPDATE_TOKEN_BALANCE].add((log['address'], from_address))
            operations[OPERATION_UPDATE_TOKEN_BALANCE].add((log['address'], to_address))

    return log, operations


def process_token_transfer_logs(logs, contracts_cache, contract=None) -> Dict[str, Set[Tuple[str]]]:
    start_time = time.monotonic()
    operations = defaultdict(set)
    for log in logs:
        log_contract = contract or contracts_cache[log['address']]
        log, log_operations = process_log(log, log_contract)

        operations[OPERATION_UPDATE_TOKEN_BALANCE] |= log_operations[OPERATION_UPDATE_TOKEN_BALANCE]
        operations[OPERATION_UPDATE_CONTRACT_INFO] |= log_operations[OPERATION_UPDATE_CONTRACT_INFO]

    logger.info(
        "%s logs of token transfer processed on %0.2f s",
        len(logs), start_time - time.monotonic()
    )
    return operations


def process_logs(logs: List[Dict[str, Any]],
                 contract: Optional[Dict[str, Any]] = None,
                 contracts_cache: Optional[Dict[str, Dict[str, Any]]] = None):
    contracts_cache = update_contract_cache(logs, contract_cache=contracts_cache or {})

    log_of_token_transfers = []
    for log in logs:
        log_contract = contract or contracts_cache.get(log['address'])
        if log_contract:
            log_of_token_transfers.append(log)
        else:
            log.update({
                'is_token_transfer': False,
                'token_transfer_to': None,
                'token_transfer_from': None,
                'token_amount': None,
                'event_type': None,
                'event_args': None,
            })
        log['is_processed'] = True

    operations = process_token_transfer_logs(log_of_token_transfers, contracts_cache, contract)
    return logs, operations
