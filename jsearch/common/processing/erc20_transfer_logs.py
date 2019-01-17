import logging
from itertools import count, chain
from typing import Dict, Set
from typing import List, Optional

from web3 import Web3

from jsearch import settings
from jsearch.common.contracts import NULL_ADDRESS
from jsearch.common.database import MainDBSync
from jsearch.common.integrations.contracts import get_contracts
from jsearch.common.rpc import ContractCall, eth_call_batch
from jsearch.typing import Log, Abi, Contract, Contracts, Logs, Transfers, Block
from jsearch.utils import split

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

    def __init__(self, token_address, account_address, block, abi, decimals):
        self.token_address = token_address
        self.account_address = account_address
        self.block = block
        self.abi = abi
        self.value = None
        self.decimals = decimals

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        if not isinstance(other, BalanceUpdate):
            raise ValueError('Expected BalanceUpdate instance')
        return self.key == other.key

    def __repr__(self):
        return f"<Balance update> {self.token_address}, {self.account_address} -> {self.value}"

    @property
    def token_as_checksum(self):
        return Web3.toChecksumAddress(self.token_address)

    @property
    def account_as_checksum(self):
        return Web3.toChecksumAddress(self.account_address)

    @property
    def is_valid(self):
        return isinstance(self.value, int)

    @property
    def key(self):
        return self.token_address, self.account_address

    def apply(self, db):
        if self.is_valid:
            db.update_token_holder_balance(self.token_address, self.account_address, self.value, self.decimals)
            logger.info(
                'Token balance updated for token %s account %s block %s value %s',
                self.token_address, self.account_address, self.block, self.value
            )
        else:
            logger.error(
                'Error when trying to update token holder balance: '
                'token %s account %s block %s balance %s decimals %s',
                self.token_address, self.account_address, self.block, self.value, self.decimals
            )


def fetch_erc20_token_decimal_bulk(contracts: Contracts) -> Contracts:
    calls = []
    counter = count()
    for contract in contracts:
        call = ContractCall(
            pk=next(counter),
            abi=contract['abi'],
            address=contract['address'],
            method='decimals',
            silent=True
        )
        calls.append(call)

    calls_results = eth_call_batch(calls=calls)
    for call, contract in zip(calls, contracts):
        contract['decimals'] = calls_results.get(call.pk)

    return contracts


def fetch_erc20_balance_bulk(updates: List[BalanceUpdate]) -> List[BalanceUpdate]:
    calls = []
    counter = count()
    for update in updates:
        call = ContractCall(
            pk=next(counter),
            abi=update.abi,
            address=update.token_as_checksum,
            method='balanceOf',
            args=[update.account_as_checksum],
            silent=True
        )
        calls.append(call)
    calls_results = eth_call_batch(calls=calls)
    for call, update in zip(calls, updates):
        update.value = calls_results.get(call.pk)
    return updates


def logs_to_balance_updates(log: Log, abi: Abi, decimals: int) -> Set[BalanceUpdate]:
    updates = set()
    if log.get('token_transfer_to'):
        to_address = log['token_transfer_to']
        from_address = log['token_transfer_from']

        block = log['block_number']
        token_address = log['address']

        if to_address != NULL_ADDRESS:
            update = BalanceUpdate(token_address, to_address, block, abi, decimals)
            updates.add(update)

        if from_address != NULL_ADDRESS:
            update = BalanceUpdate(token_address, from_address, block, abi, decimals)
            updates.add(update)

    return updates


def fetch_contracts(logs: Logs) -> Dict[str, Contract]:
    contracts = get_contracts(addresses={log['address'] for log in logs}) or []
    contracts = chain(*(fetch_erc20_token_decimal_bulk(chunk) for chunk in split(contracts, 50)))
    return {contract['address']: contract for contract in contracts}


def fetch_blocks(db: MainDBSync, logs: Logs) -> {str: Block}:
    hashes = [log['block_hash'] for log in logs]
    blocks = db.get_blocks(hashes=hashes)
    return {block['hash']: block for block in blocks}


def process_log_operations_bulk(
        db: MainDBSync,
        logs: List[Log],
        contracts: Dict[str, Contract],
        batch_size: int = settings.ETH_NODE_BATCH_REQUEST_SIZE,
) -> Logs:
    updates = set()
    for log in logs:
        contract = contracts.get(log['address'])
        if log and contract:
            abi = contract.get('abi')
            decimals = contract.get('decimals')
            updates |= logs_to_balance_updates(log, abi, decimals)

    updates = list(updates)
    for chunk in split(updates, batch_size):
        chunk = fetch_erc20_balance_bulk(chunk)

        for update in chunk:
            update.apply(db)

    return logs


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
