from aiohttp.web_request import Request
from typing import Optional, Tuple

from jsearch.api.helpers import Tag
from jsearch.typing import IntOrStr


async def get_block_number_and_timestamp(
        block_number: Optional[IntOrStr],
        timestamp: Optional[IntOrStr],
        request: Request
) -> Tuple[Optional[int], Optional[int]]:
    storage = request.app['storage']

    if {block_number, timestamp} & {Tag.LATEST}:
        last_block = await storage.get_latest_block_info()
        block_number = block_number and last_block.number
        timestamp = timestamp and last_block.timestamp

    return block_number, timestamp
