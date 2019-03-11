import logging
from itertools import count
from typing import Dict, Set
from typing import List, Optional

from web3 import Web3

from jsearch import settings
from jsearch.common.contracts import NULL_ADDRESS
from jsearch.common.rpc import ContractCall, eth_call_batch, eth_call
from jsearch.syncer.database import MainDBSync
from jsearch.typing import Log, Abi, Contract, Transfers
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
    def key(self):
        return self.token_address, self.account_address

    def apply(self, db: MainDBSync, last_block: int):
        is_valid = isinstance(self.value, int)
        if is_valid:
            changes = db.get_balance_changes_since_block(address=self.account_address, block_number=last_block)
            balance = self.value + changes

            db.update_token_holder_balance(self.token_address, self.account_address, balance, self.decimals)
        else:
            logger.error(
                'Error due to balance update: '
                'token %s account %s block %s balance %s decimals %s',
                self.token_address, self.account_address, self.block, self.value, self.decimals
            )


BalanceUpdates = List[BalanceUpdate]


def fetch_erc20_token_balance(contract_abi: Abi,
                              token_address: str,
                              account_address: str,
                              block: str = 'latest') -> int:
    token_address_checksum = Web3.toChecksumAddress(token_address)
    account_address_checksum = Web3.toChecksumAddress(account_address)

    call = ContractCall(
        abi=contract_abi,
        address=token_address_checksum,
        method='balanceOf',
        args=[account_address_checksum, ],
        block=block,
        silent=True
    )

    return eth_call(call=call)


def fetch_erc20_balance_bulk(updates: BalanceUpdates, block: Optional[int] = None) -> BalanceUpdates:
    calls = []
    counter = count()
    for update in updates:
        call = ContractCall(
            pk=next(counter),
            abi=update.abi,
            address=update.token_as_checksum,
            method='balanceOf',
            args=[update.account_as_checksum],
            block=block,
            silent=True
        )
        calls.append(call)
    calls_results = eth_call_batch(calls=calls)
    for call, update in zip(calls, updates):
        update.value = calls_results.get(call.pk)
    return updates


def logs_to_balance_updates(log: Log, abi: Abi, decimals: int) -> Set[BalanceUpdate]:
    updates = set()
    to_address = log['to_address']
    from_address = log['from_address']

    block = log['block_number']
    token_address = log['token_address']

    if to_address != NULL_ADDRESS:
        update = BalanceUpdate(token_address, to_address, block, abi, decimals)
        updates.add(update)

    update = BalanceUpdate(token_address, from_address, block, abi, decimals)
    updates.add(update)

    return updates


def update_token_holder_balances(
        db: MainDBSync,
        transfers: Transfers,
        contracts: Dict[str, Contract],
        last_block: int,
        batch_size: int = settings.ETH_NODE_BATCH_REQUEST_SIZE,
) -> None:
    updates = set()
    for transfer in transfers:
        contract_address = transfer['token_address']
        contract = contracts.get(contract_address)
        if transfer and contract:
            abi = contract.get('abi')
            decimals = contract.get('decimals')
            updates |= logs_to_balance_updates(transfer, abi, decimals)

    for chunk in split(updates, batch_size):
        updates = fetch_erc20_balance_bulk(chunk)
        for update in updates:
            update.apply(db, last_block)
