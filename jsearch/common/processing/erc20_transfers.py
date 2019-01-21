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
        'timestamp': block['timestamp'],
        'to_address': log['token_transfer_to'],
        'token_address': log['address'],
        'token_decimals': contract['decimals'],
        'token_name': contract['token_name'],
        'token_symbol': contract['token_symbol'],
        'token_value': log['token_amount'],
        'transaction_hash': log['transaction_hash']
    }
    return [
        {'address': log['token_transfer_to'], **transfer_body},
        {'address': log['token_transfer_from'], **transfer_body}
    ]


def logs_to_transfers(logs: Logs, blocks: Dict[str, Block], contracts: Dict[str, Contract]) -> Transfers:
    transfers = []
    for log in logs:
        contract = contracts.get(log['address'])
        block = blocks.get(log['block_hash'])
        if block and log and contract and log.get('is_token_transfer'):
            transfers.extend(log_to_transfers(log, block, contract))
    return transfers
