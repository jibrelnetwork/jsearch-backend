from typing import NamedTuple, Optional


class BlockInfo(NamedTuple):
    hash: str
    number: int


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
