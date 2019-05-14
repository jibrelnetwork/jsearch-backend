from typing import Optional

from jsearch.common.contracts import NULL_ADDRESS, ERC20_METHODS_IDS
from jsearch.wallet_worker.consts import WalletEventType, CANCELLATION_ADDRESS, TOKEN_DECIMALS_DEFAULT
from jsearch.wallet_worker.typing import Transaction, Event, TokenTransfer, InternalTransaction, PendingTransaction


def get_event_type(tx_data: Transaction) -> Optional[str]:
    if int(tx_data['value'], 16) != 0:
        return WalletEventType.ETH_TRANSFER

    if tx_data['to'] == NULL_ADDRESS:
        return WalletEventType.CONTRACT_CALL

    if tx_data.get('to_contract') is True:
        method_id = tx_data['input'][:10]
        if method_id in (ERC20_METHODS_IDS['transferFrom'], ERC20_METHODS_IDS['transfer']):
            return WalletEventType.CONTRACT_CALL
        else:
            return None
    else:
        if tx_data['to'] == CANCELLATION_ADDRESS:
            return WalletEventType.TX_CANCELLATION
    return None


def event_from_tx(address: str, tx_data: Transaction) -> Event:
    """
    Make wallet event object from TX data

    :param  address: from address of to address of Transaction - explicitly
    :param tx_data: full TX data object

    :return: event data object
    """
    event_type = get_event_type(tx_data)
    if event_type:
        return {
            'is_forked': False,
            'address': address,
            'type': event_type,
            'block_number': tx_data['block_number'],
            'block_hash': tx_data['block_hash'],
            'tx_hash': tx_data['hash'],
            'event_index': tx_data['transaction_index'] + tx_data['block_number'] * 1000,
            'tx_data': tx_data,
            'event_data': {
                'sender': tx_data['from'],
                'recipient': tx_data['to'],
                'amount': str(int(tx_data['value'], 16)),
                'status': tx_data['receipt_status']
            }
        }


def event_from_token_transfer(address: str, transfer_data: TokenTransfer, tx_data: Transaction) -> Event:
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
    decimals = transfer_data['token_decimals'] or TOKEN_DECIMALS_DEFAULT
    amount = str(transfer_data['token_value'] / 10 ** decimals)
    event_data = {
        'is_forked': False,
        'address': address,
        'type': event_type,
        'block_number': tx_data['block_number'],
        'block_hash': tx_data['block_hash'],
        'tx_hash': tx_data['hash'],
        'event_index': tx_data['transaction_index'] + tx_data['block_number'] * 1000,
        'tx_data': tx_data,
        'event_data': {
            'sender': transfer_data['from_address'],
            'recipient': transfer_data['to_address'],
            'amount': amount,
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
    if tx_data['receipt_status'] == 0 or internal_tx_data['status'] != 'success':
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
        'event_index': tx_data['transaction_index'] + tx_data['block_number'] * 1000,
        'tx_data': tx_data,
        'event_data': {
            'sender': internal_tx_data['from'],
            'recipient': internal_tx_data['to'],
            'amount': str(internal_tx_data['value']),
            'status': event_status
        }
    }
    return event_data


def get_event_from_pending_tx(address: str, event_index: int, pending_tx: PendingTransaction) -> Event:
    event_type = get_event_type(pending_tx)
    if event_type:
        return {
            'is_removed': pending_tx['removed'],
            'address': address,
            'type': event_type,
            'tx_hash': pending_tx['hash'],
            'tx_data': pending_tx,
            'event_index': event_index,
            'event_data': {
                'sender': pending_tx['from'],
                'recipient': pending_tx['to'],
                'amount': str(int(pending_tx['value'], 16)),
            }
        }
