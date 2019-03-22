import logging
from itertools import count
from typing import Dict, Set, Union
from typing import List, Optional

from web3 import Web3

from jsearch import settings
from jsearch.common.contracts import NULL_ADDRESS
from jsearch.common.last_block import LastBlock
from jsearch.common.rpc import ContractCall, eth_call_batch, eth_call
from jsearch.syncer.database import MainDBSync
from jsearch.typing import Log, Abi, Contract, Transfers
from jsearch.utils import split
from jsearch.service_bus import sync_client

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
        changes = None
        balance = None

        is_valid = isinstance(self.value, int)
        if is_valid:

            if last_block != LastBlock.LATEST_BLOCK:
                changes = db.get_balance_changes_since_block(
                    token=self.token_address,
                    account=self.account_address,
                    block_number=last_block
                )
            else:
                changes = 0

            balance = self.value + changes

            is_valid = balance >= 0
            if is_valid:
                db.update_token_holder_balance(self.token_address, self.account_address, balance, self.decimals)
                logger.info(
                    '[BALANCE UPDATE] %s on block %s token %s address %s -> %30d + %30d : %30d',
                    self.block, last_block, self.token_address, self.account_address, self.value, changes, balance
                )

        if not is_valid:
            logger.error(
                '[BALANCE UPDATE ERROR] %s on block %s token %s address %s -> %30s + %30s : %30s',
                self.block, last_block, self.token_address, self.account_address, self.value, changes, balance
            )

    def to_asset_update(self):
        return {
            'asset_address': self.token_address,
            'address': self.account_address,
            'balance': self.value / (10 ** self.decimals) if (self.value and self.decimals) else None}


BalanceUpdates = List[BalanceUpdate]


def fetch_erc20_token_balance(contract_abi: Abi,
                              token_address: str,
                              account_address: str,
                              block: Optional[Union[str, int]] = 'latest') -> int:
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


def fetch_erc20_balance_bulk(updates: BalanceUpdates, block: Optional[Union[str, int]] = None) -> BalanceUpdates:
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

    if from_address != NULL_ADDRESS:
        update = BalanceUpdate(token_address, from_address, block, abi, decimals)
        updates.add(update)

    return updates


def update_token_holder_balances(
        db: MainDBSync,
        transfers: Transfers,
        contracts: Dict[str, Contract],
        last_block: Union[int, str],
        batch_size: int = settings.ETH_NODE_BATCH_REQUEST_SIZE,
) -> None:
    updates = set()
    for transfer in transfers:
        contract_address = transfer['token_address']
        contract = contracts.get(contract_address)
        if contract:
            abi = contract.get('abi')
            decimals = contract.get('decimals')
            updates |= logs_to_balance_updates(transfer, abi, decimals)
        else:
            logger.info('[BALANCE UPDATE ERROR] Contract was not found %s', contract_address)

    for chunk in split(updates, batch_size):
        updates = fetch_erc20_balance_bulk(chunk, block=last_block)
        for update in updates:
            update.apply(db, last_block)
        sync_client.write_assets_updates([u.to_asset_update() for u in updates if u.value])

