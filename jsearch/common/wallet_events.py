import logging

from typing import Optional, Tuple

from jsearch.common.contracts import NULL_ADDRESS, ERC20_METHODS_IDS
from jsearch.typing import Transaction, Event, Transfer, InternalTransaction, PendingTransaction

CANCELLATION_ADDRESS = '0x000000000000000000000063616e63656c6c6564'
TOKEN_DECIMALS_DEFAULT = 18

logger = logging.getLogger()


class WalletEventType:
    ERC20_TRANSFER = 'erc20-transfer'
    ETH_TRANSFER = 'eth-transfer'
    CONTRACT_CALL = 'contract-call'
    TX_CANCELLATION = 'tx-cancellation'


def get_event_type(tx_data: Transaction, is_receiver_contract=False, is_pending=False) -> Optional[str]:
    """
    Accord to https://jibrelnetwork.atlassian.net/wiki/spaces/JWALLET/pages/769327162/Ethereum+blockchain+events
    """
    if int(tx_data['value'], 16) != 0:
        return WalletEventType.ETH_TRANSFER

    if tx_data['to'] == CANCELLATION_ADDRESS:
        return WalletEventType.TX_CANCELLATION

    if tx_data['to'] == NULL_ADDRESS:
        return WalletEventType.CONTRACT_CALL

    method_id = tx_data.get('input', "")[:10]
    is_it_transfer = method_id in (ERC20_METHODS_IDS['transferFrom'], ERC20_METHODS_IDS['transfer'])

    if is_it_transfer and (is_receiver_contract or is_pending):
        return WalletEventType.ERC20_TRANSFER

    if method_id and (is_receiver_contract or is_pending):
        return WalletEventType.CONTRACT_CALL

    return None


def event_from_tx(address: str, tx_data: Transaction, is_receiver_contract=False) -> Event:
    """
    Make wallet event object from TX data

    Args:
        address: from address of to address of Transaction - explicitly
        tx_data: full TX data object
        is_receiver_contract: bool flag

    Returns:
        event data object
    """
    event_type = get_event_type(tx_data, is_receiver_contract=is_receiver_contract)
    if event_type:
        return {
            'is_forked': False,
            'address': address,
            'type': event_type,
            'block_number': tx_data['block_number'],
            'block_hash': tx_data['block_hash'],
            'tx_hash': tx_data['hash'],
            'event_index': make_event_index_for_tx(tx_data['block_number'], tx_data['transaction_index']),
            'tx_data': None,
            'event_data': {
                'sender': tx_data['from'],
                'recipient': tx_data['to'],
                'amount': str(int(tx_data['value'], 16)),
                'status': tx_data['status']
            }
        }


def event_from_token_transfer(address: str, transfer_data: Transfer, tx_data: Transaction) -> Event:
    """
    Make wallet event object from Transfer and TX data

    Args:
        address: from address or to address of Transrer - explicitly
        tx_data: full TX data object
        transfer_data: full Token Transfer data object

    Returns:
        event data object
    """
    event_type = WalletEventType.ERC20_TRANSFER
    decimals = TOKEN_DECIMALS_DEFAULT if transfer_data['token_decimals'] is None else transfer_data['token_decimals']

    amount = str(transfer_data['token_value'])
    decimals = str(decimals)

    event_data = {
        'is_forked': False,
        'address': address,
        'type': event_type,
        'block_number': tx_data['block_number'],
        'block_hash': tx_data['block_hash'],
        'tx_hash': tx_data['hash'],
        'event_index': make_event_index_for_log(
            tx_data['block_number'],
            tx_data['transaction_index'],
            transfer_data['log_index']
        ),
        'tx_data': None,
        'event_data': {
            'sender': transfer_data['from_address'],
            'recipient': transfer_data['to_address'],
            'amount': amount,
            'decimals': decimals,
            'asset': transfer_data['token_address'],
            'status': transfer_data['status']
        }
    }
    return event_data


def event_from_internal_tx(address: str,
                           internal_tx_data: InternalTransaction,
                           tx_data: Transaction) -> Optional[Event]:
    """
    Make wallet event object from internal TX data and Root TX data

    Args:
        address: from address or to address of Transaction - explicitly
        internal_tx_data: internal TX data object
        tx_data: full TX data object

    Returns:
        event data object
    """
    if internal_tx_data['value'] == 0:
        return None

    event_type = WalletEventType.ETH_TRANSFER
    if tx_data['status'] == 0 or internal_tx_data['status'] != 'success':
        event_status = 0
    else:
        event_status = 1

    event_data = {
        'is_forked': False,
        'address': address,
        'type': event_type,
        'block_number': tx_data['block_number'],
        'block_hash': tx_data['block_hash'],
        'tx_hash': tx_data['hash'],
        'event_index': make_event_index_for_internal_tx(
            tx_data['block_number'],
            tx_data['transaction_index'],
            internal_tx_data['transaction_index']
        ),
        'tx_data': None,
        'event_data': {
            'sender': internal_tx_data['from'],
            'recipient': internal_tx_data['to'],
            'amount': str(internal_tx_data['value']),
            'status': event_status
        }
    }
    return event_data


def get_token_transfer_args_from_pending_tx(pending_tx: PendingTransaction) -> Tuple[str, str, str]:
    try:
        tx_input = pending_tx['input']
        method_id = tx_input[:10]
        body = tx_input[10:]

        amount = str(int(body[-64:], 16))
        if method_id == ERC20_METHODS_IDS['transferFrom']:
            # signature is `function transferFrom(address from, address to, uint tokens)`

            sender = f"0x{body[:64][-40:]}"
            receiver = f"0x{body[64:][-40:]}"

        elif method_id == ERC20_METHODS_IDS['transfer']:
            # signature is `function transfer(address to, uint tokens)`

            sender = pending_tx["from"]
            receiver = f"0x{body[:64][-40:]}"
        else:
            amount = sender = receiver = None

    except (TypeError, IndexError) as e:
        logger.error(e)
        amount = sender = receiver = None

    return sender, receiver, amount


def get_event_from_pending_tx(address: str, pending_tx: PendingTransaction) -> Event:
    event_type = get_event_type(pending_tx, is_pending=True)

    if event_type == WalletEventType.ERC20_TRANSFER:
        sender, recipient, amount = get_token_transfer_args_from_pending_tx(pending_tx)
    else:
        sender = pending_tx['from']
        recipient = pending_tx['to']
        amount = str(int(pending_tx['value'], 16))

    if event_type:
        return {
            'is_removed': pending_tx['removed'],
            'address': address,
            'type': event_type,
            'tx_hash': pending_tx['hash'],
            'tx_data': None,
            'event_index': 0,
            'event_data': {
                'sender': sender,
                'recipient': recipient,
                'amount': amount,
            }
        }


# Logs, TXs and internal TXs share last four digits of the event index:
#   * 0000 occupied by TXs.
#   * 0001..4999 occupied by internal TXs' indices.
#   * 5000..9999 occupied by logs' indices.
#
# Logs must occupy 5000..9999 because they are enumerated starting from zero by
# geth-fork and log with index 0 can lead to collision with event from plain
# transaction which also has `item_index=0`.


def make_event_index_for_tx(block_number: int, transaction_index: int) -> int:
    return make_event_index(block_number, transaction_index, item_index=0)


def make_event_index_for_log(block_number: int, transaction_index: int, log_index: int) -> int:
    return make_event_index(block_number, transaction_index, item_index=5000 + log_index)


def make_event_index_for_internal_tx(block_number: int, transaction_index: int, internal_tx_index: int) -> int:
    return make_event_index(block_number, transaction_index, item_index=internal_tx_index)


def make_event_index(
        block_number: int,
        transaction_index: int,
        item_index: int,
) -> int:
    """
    This functions forms an event index based on different input params.

    Event index is used for sorting wallet events and consists of:
      * Block number.
      * TX index.
      * Item index (either a log or an internal TX index or zero for TX).

    Examples:
        >>> make_event_index(7800000, 230, 0)  # From TX.
        78000002300000
        >>> make_event_index(7800000, 230, 5012)  # From log.
        78000002305012
        >>> make_event_index(8500000, 187, 42)  # From internal TX.
        85000001870042
    """
    return block_number * 10000000 + transaction_index * 10000 + item_index
