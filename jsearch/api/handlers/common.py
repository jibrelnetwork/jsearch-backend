from typing import Optional, Tuple

from jsearch.api.error_code import ErrorCode
from jsearch.api.helpers import Tag, ApiError
from jsearch.api.ordering import ORDER_DESC
from jsearch.api.storage import Storage
from jsearch.typing import IntOrStr, OrderDirection


async def get_block_number_or_tag_from_timestamp(
        storage: Storage,
        timestamp: Optional[IntOrStr],
        direction: OrderDirection
) -> Optional[IntOrStr]:
    if {timestamp} & {None, Tag.TIP, Tag.LATEST}:
        return timestamp

    block_info = await storage.get_block_by_timestamp(timestamp, direction)  # type: ignore
    if block_info:
        block_number: Optional[int] = block_info.number
    else:
        if direction == ORDER_DESC:
            block_number, _ = await get_last_block_number_and_timestamp(Tag.LATEST, None, storage)
        else:
            block_number = 0
    return block_number


async def get_last_block_number_and_timestamp(
        block_number: Optional[IntOrStr],
        timestamp: Optional[IntOrStr],
        storage: Storage
) -> Tuple[Optional[int], Optional[int]]:
    if {block_number, timestamp} & {Tag.LATEST}:
        last_block = await storage.get_latest_block_info()
        block_number = block_number and last_block and last_block.number  # type: ignore
        timestamp = timestamp and last_block and last_block.timestamp  # type: ignore

    return block_number, timestamp  # type: ignore


async def get_tip_block_number_and_timestamp(
        block_number: Optional[IntOrStr],
        timestamp: Optional[IntOrStr],
        tip_hash: Optional[str],
        storage: Storage
) -> Tuple[Optional[int], Optional[int]]:
    if {block_number, timestamp} & {Tag.TIP}:
        if tip_hash is None:
            raise ApiError(
                {
                    'field': 'tip',
                    'code': ErrorCode.BLOCK_NOT_FOUND,
                    'message': f'Block with hash {tip_hash} not found'
                },
                status=400
            )

        block_info = await storage.get_block_info(tip_hash)
        if block_info is None:
            raise ApiError(
                {
                    'field': 'tip',
                    'code': ErrorCode.BLOCK_NOT_FOUND,
                    'message': f'Block with hash {tip_hash} not found'
                },
                status=400
            )
        block_number = block_number and block_info.number
        timestamp = timestamp and block_info.timestamp

    return block_number, timestamp  # type: ignore
