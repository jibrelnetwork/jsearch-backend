import logging

from typing import Dict

from jsearch.typing import Log, Contract, Logs, Transfers, Block

logger = logging.getLogger(__name__)


def log_to_transfers(log: Log, block: Block, contract: Contract) -> Transfers:
    transfer_body = {
        'block_hash': log['block_hash'],
        'block_number': log['block_number'],
        'from_address': log['token_transfer_from'],
        'log_index': log['log_index'],
        'timestamp': block and block['timestamp'],
        'to_address': log['token_transfer_to'],
        'token_address': log['address'],
        'token_decimals': contract and contract['decimals'],
        'token_name': contract and contract['token_name'],
        'token_symbol': contract and contract['token_symbol'],
        'token_value': log['token_amount'],
        'transaction_hash': log['transaction_hash'],
        'transaction_index': log['transaction_index'],
        'is_forked': block['is_forked'],
        'status': log['status']
    }

    # FAQ: Construct 2 entries for every transfer event to optimize performance.
    # In a `SELECT * FROM t WHERE c1={address} or c2={address}` `or` clause hits
    # performance too hard, so add two entries with denormalized `address`. That
    # way, we can fetch all transfers with a simple query without `or`:
    # `SELECT * FROM t WHERE address={address}`.
    return [
        {'address': log['token_transfer_to'], **transfer_body},
        {'address': log['token_transfer_from'], **transfer_body}
    ]


def logs_to_transfers(logs: Logs, block: Block, decimals: Dict[str, int]) -> Transfers:
    transfers = []
    for log in logs:
        if log and log.get('is_token_transfer'):
            contract = {
                'decimals': decimals.get(log['address']),
                'token_name': None,
                'token_symbol': None
            }
            log_transfers = log_to_transfers(log, block, contract)
            transfers.extend(log_transfers)
    return transfers
