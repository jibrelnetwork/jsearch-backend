import logging
from typing import Dict

from jsearch.typing import Log, Contract, Logs, Transfers, Block

logger = logging.getLogger(__name__)


def log_to_transfers(log: Log, block: Block, contract: Contract) -> Transfers:
    """
    We should use block as primary sources about forked state.
    It's may lead to some confuse, but logs will come from service bus queue, where their are immutable.
    It may lead to next case:
        given:
            - syncer handles new batch of transaction logs.
            - syncer writes transaction logs to service bus
            - syncer process reorganisation record with block
                which contains our transaction and mark logs as forked
        when:
            - worker handles transaction logs from queue and save token transfers
        then:
            - transaction logs was changed in database
            - transaction logs wasn't changed in queue
            - token transfers aren't forked, because logs in queue also aren't forked
    As result of this case we get broken data.
    To avoid this case we decide - block record is a primary source about forked sign.
    """
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
        'is_forked': block['is_forked']
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


def logs_to_transfers(logs: Logs, blocks: Dict[str, Block], contracts: Dict[str, Contract]) -> Transfers:
    transfers = []
    for log in logs:
        if log and log.get('is_token_transfer'):
            block = blocks.get(log['block_hash'])
            contract = contracts.get(log['address'])

            log_transfers = log_to_transfers(log, block, contract)
            transfers.extend(log_transfers)
    return transfers
