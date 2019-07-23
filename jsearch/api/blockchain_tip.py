from typing import Optional

from jsearch.api.error_code import ErrorCode
from jsearch.api.helpers import ApiError
from jsearch.api.storage import Storage
from jsearch.api.structs import BlockchainTip, BlockInfo


async def get_tip_or_raise_api_error(
        storage: Storage,
        tip_hash: str,
        last_block: Optional[BlockInfo] = None,
) -> BlockchainTip:

    tip_block = await storage.get_block_info(tip_hash)

    if tip_block is None:
        raise ApiError(
            {
                'field': 'tip',
                'error_code': ErrorCode.BLOCK_NOT_FOUND,
                'error_message': f'Block with hash {tip_hash} not found'
            },
            status=404
        )

    return await storage.get_blockchain_tip(tip_block, last_block)


def is_tip_stale(tip: Optional[BlockchainTip], block_number: int) -> bool:
    if tip is None:
        # WTF: `BlockchainTip` is not a required query param and can be omitted
        # by clients. If it was omitted, data is never stale.
        return False

    return (
        tip.is_in_fork and
        tip.last_unchanged_block is not None and
        tip.last_unchanged_block < block_number
    )
