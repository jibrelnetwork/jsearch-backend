from attr import dataclass
from typing import NamedTuple, Optional, List

from jsearch.typing import Columns, OrderDirection, OrderScheme


class Ordering(NamedTuple):
    columns: Columns
    fields: List[str]
    scheme: OrderScheme
    direction: OrderDirection


@dataclass
class BlockInfo:
    hash: str
    number: int
    timestamp: Optional[int] = None


class BlockchainTip(NamedTuple):
    # fields for input tip
    tip_hash: Optional[str]
    tip_number: Optional[int]

    # fields for last block data
    last_hash: str
    last_number: int

    # tips
    is_in_fork: bool
    last_unchanged_block: Optional[int]

    def to_dict(self):
        return {
            'currentBlockchainTip': {
                'blockHash': self.last_hash,
                'blockNumber': self.last_number
            },
            'blockchainTipStatus': {
                'isOrphaned': self.is_in_fork,
                'blockHash': self.tip_hash,
                'blockNumber': self.tip_number,
                'lastUnchangedBlock': self.last_unchanged_block
            }
        }

    def __repr__(self):
        return (f"<BlockchainTip "
                f"tip={self.tip_hash} "
                f"is_fork={self.is_in_fork} "
                f"last_unchanged={self.last_unchanged_block} />")


class AssetSummary(NamedTuple):
    address: str
    balance: str
    decimals: str
    transfers_number: str

    def to_dict(self):
        return {
            'address': self.address,
            'balance': self.balance,
            'decimals': self.decimals,
            'transfersNumber': self.transfers_number
        }


AssetsSummary = List[AssetSummary]


class AddressSummary(NamedTuple):
    address: str
    assets_summary: AssetsSummary
    outgoing_transactions_number: str

    def to_dict(self):
        return {
            'address': self.address,
            'assetsSummary': [item.to_dict() for item in self.assets_summary],
            'outgoingTransactionsNumber': self.outgoing_transactions_number
        }


AddressesSummary = List[AddressSummary]
