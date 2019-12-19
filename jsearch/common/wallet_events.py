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
    # WTF:
    #   * `"transactions"."value"` is hex-string
    #   * `"pending_transactions"."value"` is dec-string
    value_base = 10 if is_pending else 16
    value = int(tx_data['value'], value_base)

    if value != 0:
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


def event_from_tx(address: str, tx_data: Transaction, is_receiver_contract=False) -> Optional[Event]:
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

    return None


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


def get_token_transfer_args_from_pending_tx(
        pending_tx: PendingTransaction,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    >>> get_token_transfer_args_from_pending_tx(  # 'transfer'
    ...     {
    ...         'input': '0xa9059cbb0000000000000000000000004aeff5f0ffcfb9db8d'
    ...                  '0e3eec2f44df7a96d41ba5000000000000000000000000000000'
    ...                  '00000000000000000000000002a4aa0cc0',
    ...         'from': '0xbf9d709cca3a5eae0a07791bebed674bbbd43b6d',
    ...     }
    ... ) == (
    ...     '0xbf9d709cca3a5eae0a07791bebed674bbbd43b6d',
    ...     '0x4aeff5f0ffcfb9db8d0e3eec2f44df7a96d41ba5',
    ...     '11352542400',
    ... )
    True
    >>> get_token_transfer_args_from_pending_tx(  # 'transferFrom'
    ...     {
    ...         'input': '0x23b872dd00000000000000000000000005723a8d81b06330da'
    ...                  '7dee6b1f6dab7c422b54b90000000000000000000000002faf48'
    ...                  '7a4414fe77e2327f0bf4ae2a264a776ad2000000000000000000'
    ...                  '00000000000000000000000000006c6884f5878acd8000',
    ...     }
    ... ) == (
    ...     '0x05723a8d81b06330da7dee6b1f6dab7c422b54b9',
    ...     '0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2',
    ...     '1999779774400000000000',
    ... )
    True
    >>> get_token_transfer_args_from_pending_tx({'input': '0x', 'from': '0xbf9d709cca3a5eae0a07791bebed674bbbd43b6d'})
    (None, None, None)
    >>> get_token_transfer_args_from_pending_tx({'input': '0x'})
    (None, None, None)
    """
    try:
        tx_input = pending_tx['input']
        method_id = tx_input[:10]
        body = tx_input[10:]

        amount: Optional[str] = str(int(body[-64:], 16))
        if method_id == ERC20_METHODS_IDS['transferFrom']:
            # signature is `function transferFrom(address from, address to, uint tokens)`

            sender: Optional[str] = f"0x{body[:64][-40:]}"
            receiver: Optional[str] = f"0x{body[64:128][-40:]}"

        elif method_id == ERC20_METHODS_IDS['transfer']:
            # signature is `function transfer(address to, uint tokens)`

            sender = pending_tx["from"]
            receiver = f"0x{body[:64][-40:]}"
        else:
            amount = sender = receiver = None

    except (TypeError, IndexError, ValueError):
        logger.exception('Cannot retrieve transfer args from pending TX')
        amount = sender = receiver = None

    return sender, receiver, amount


def get_event_from_pending_tx(address: str, pending_tx: PendingTransaction) -> Optional[Event]:
    event_type = get_event_type(pending_tx, is_pending=True)

    if event_type == WalletEventType.ERC20_TRANSFER:
        sender, recipient, amount = get_token_transfer_args_from_pending_tx(pending_tx)
        event_data = {
            'sender': sender,
            'recipient': recipient,
            'amount': amount,
            'asset': pending_tx['to'],
        }
    else:
        event_data = {
            'sender': pending_tx['from'],
            'recipient': pending_tx['to'],
            'amount': pending_tx['value'],
        }

    if event_type:
        return {
            'is_removed': pending_tx['removed'],
            'address': address,
            'type': event_type,
            'tx_hash': pending_tx['hash'],
            'tx_data': None,
            'event_index': 0,
            'event_data': event_data,
        }

    return None


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
