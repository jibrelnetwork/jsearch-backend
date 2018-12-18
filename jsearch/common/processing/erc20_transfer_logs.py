import logging
from typing import List, Optional
from typing import Tuple, Dict, Any, Set

from hexbytes import HexBytes
from web3 import Web3
from web3.utils.contracts import prepare_transaction

from jsearch import settings
from jsearch.common.contracts import ERC20_ABI
from jsearch.common.contracts import NULL_ADDRESS
from jsearch.common.database import MainDBSync
from jsearch.common.integrations.contracts import get_contract
from jsearch.common.operations import update_token_info
from jsearch.common.processing.logs import EventTypes, TRANSFER_EVENT_INPUT_SIZE
from jsearch.common.rpc import BatchHTTPProvider, decode_erc20_output_value
from jsearch.typing import Log, Contract, EventArgs, Abi
from jsearch.utils import suppress_exception

logger = logging.getLogger(__name__)


class BalanceUpdate:
    token_address: str
    account_address: str
    block: int

    value: Optional[int]
    decimals: Optional[int]

    __slots__ = (
        'abi',
        'token_address',
        'account_address',
        'block',
        'decimals',
        'value'
    )

    def __init__(self, token_address, account_address, block, abi):
        self.token_address = token_address
        self.account_address = account_address
        self.block = block
        self.abi = abi

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        if not isinstance(other, BalanceUpdate):
            raise ValueError('Expected BalanceUpdate instance')
        return self.key == other.key

    def __repr__(self):
        return f"<Balance update> {self.token_address}, {self.account_address} -> {self.balance}"

    @property
    def token_as_checksum(self):
        return Web3.toChecksumAddress(self.token_address)

    @property
    def account_as_checksum(self):
        return Web3.toChecksumAddress(self.account_address)

    @property
    def is_failed(self):
        return self.decimals is None or self.value is None

    @property
    def balance(self):
        if not self.is_failed:
            return self.value / 10 ** self.decimals

    @property
    def key(self):
        return self.token_address, self.account_address

    def apply(self, db):
        if not self.is_failed:
            db.update_token_holder_balance(self.token_address, self.account_address, self.value)
            logger.info(
                'Token balance updated for token %s account %s block %s value %s',
                self.token_address, self.account_address, self.block, self.balance
            )
        else:
            logger.error(
                'Error when trying to update token holder balance for token %s account %s block %s',
                self.token_address, self.account_address, self.block
            )


def get_request_provider():
    return BatchHTTPProvider(settings.ETH_NODE_URL)


def fetch_erc20_token_decimal_bulk(updates: List[BalanceUpdate]) -> List[BalanceUpdate]:
    request_provider = get_request_provider()
    w3 = Web3(request_provider)

    calls_params = []
    for update in updates:
        tx = prepare_transaction(
            abi=update.abi,
            address=update.token_as_checksum,
            web3=w3,
            fn_identifier='decimals'
        )
        calls_params.append([tx, "latest"])

    responses = request_provider.make_request('eth_call', params=calls_params)

    for i, response in enumerate(responses):
        update = updates[i]
        update.decimals = decode_erc20_output_value(
            data=HexBytes(response['result']),
            fn_identifier='decimals',
            abi=update.abi,
        )

    return updates


def fetch_erc20_balance_bulk(updates: List[BalanceUpdate]) -> List[BalanceUpdate]:
    request_provider = get_request_provider()
    w3 = Web3(request_provider)

    calls_params = []
    for update in updates:
        tx = prepare_transaction(
            abi=ERC20_ABI,
            address=update.token_as_checksum,
            web3=w3,
            fn_identifier='balanceOf',
            fn_args=(update.account_as_checksum,)
        )
        calls_params.append([tx, "latest"])

    responses = request_provider.make_request('eth_call', params=calls_params)

    for i, response in enumerate(responses):
        update = updates[i]
        update.value = decode_erc20_output_value(
            data=HexBytes(response["result"]),
            abi=update.abi,
            fn_identifier='balanceOf',
            args=(update.account_as_checksum,),
        )

    return updates


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
        token_decimals: int
) -> Tuple[str, str, str]:
    """
    Eject transfer details from event
    """
    event_inputs = get_event_inputs_from_abi(abi)

    args_keys = [interface_type['name'] for interface_type in event_inputs]
    args_values = [event_args[key] for key in args_keys]

    from_address = args_values[0]
    to_address = args_values[1]
    token_amount = args_values[2] / (10 ** token_decimals)

    return from_address, to_address, token_amount


@suppress_exception
def process_log_transfer(log: Log, contract: Optional[Contract] = None) -> Tuple[Log, Abi]:
    event_args = log['event_args']
    contract: Optional[Contract] = contract or get_contract(log['address'])

    abi = None
    if event_args and contract:
        abi: Abi = contract['abi']
        token_decimals = contract['token_decimals']

        if token_decimals is None:
            contract_address = contract['address']
            update_token_info(contract_address, abi)

        elif log.get('is_token_transfer'):
            from_address, to_address, token_amount = get_transfer_details_from_erc20_event_args(
                event_args=event_args, abi=ERC20_ABI, token_decimals=token_decimals
            )
            log.update({
                'token_transfer_to': to_address,
                'token_transfer_from': from_address,
                'token_amount': token_amount,
            })
    log['is_transfer_processed'] = True
    return log, abi


def logs_to_balance_updates(log: Log, abi: Abi) -> Set[BalanceUpdate]:
    updates = set()
    if log.get('token_transfer_to'):
        to_address = log['token_transfer_to']
        from_address = log['token_transfer_from']

        block = log['block_number']
        token_address = log['address']

        if to_address != NULL_ADDRESS:
            update = BalanceUpdate(token_address=token_address, account_address=to_address, block=block, abi=abi)
            updates.add(update)

        if from_address != NULL_ADDRESS:
            update = BalanceUpdate(token_address=token_address, account_address=from_address, block=block, abi=abi)
            updates.add(update)

    return updates


def process_log_operations_bulk(
        db: MainDBSync,
        logs: List[Log],
        contract: Optional[Contract] = None,
        batch_size: int = settings.ETH_NODE_BATCH_REQUEST_SIZE,
) -> None:
    logs = [process_log_transfer(log, contract) for log in logs]
    logs = (log for log in logs if log)

    updates = set()
    for log, abi in logs:
        if log:
            updates |= logs_to_balance_updates(log, abi)

    updates = list(updates)
    for offset in range(0, len(updates), batch_size):
        chunk = updates[offset:offset + batch_size]

        chunk = fetch_erc20_token_decimal_bulk(chunk)
        chunk = fetch_erc20_balance_bulk(chunk)

        for update in chunk:
            update.apply(db)
