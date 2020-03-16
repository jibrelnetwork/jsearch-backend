import logging
from typing import Dict, Any, Optional, List, Coroutine, Callable

from jsearch.common.utils import timeit
from jsearch.syncer.database import MainDB, RawDB
from jsearch.syncer.processor import process_block
from jsearch.syncer.structs import RawBlockData

logger = logging.getLogger(__name__)


async def safe_get_block_and_maybe_load(
        block_hash: str,
        blocks: Dict[str, Dict[str, Any]],
        load_missed=Optional[Callable[[str], Coroutine[Any, Any, None]]],
) -> Dict[str, Any]:
    try:
        block = blocks[block_hash]
    except KeyError:
        if load_missed is None:
            raise

        block = await load_missed(block_hash)
    return block


async def get_block_chain_from_split(
        blocks: Dict[str, Dict[str, Any]],
        split: Dict[str, Any],
        load_missed=Optional[Callable[[str], Coroutine[Any, Any, None]]],
        *, direction: str
) -> List[Dict[str, Any]]:
    assert direction in ('add', 'drop')

    depth: int = split[f'{direction}_length']
    head: str = split[f'{direction}_block_hash']

    chain = [await safe_get_block_and_maybe_load(head, blocks, load_missed)]

    while len(chain) < depth:
        last_link = chain[-1]['parent_hash']
        next_block = await safe_get_block_and_maybe_load(last_link, blocks, load_missed)
        chain.append(next_block)
    return chain


def get_split_range(split_data: Dict[str, Any]):
    from_block: int = split_data['block_number']
    to_block = from_block + max(split_data['add_length'] or 0, split_data['drop_length'] or 0)
    return from_block, to_block


async def load_and_apply_create_event(
        raw_db: RawDB,
        main_db: MainDB,
        block_hash: str,
        node_id: str
) -> Dict[str, Any]:
    logging.info('Load and insert block', extra={'hash': block_hash})
    insert_event = await raw_db.get_insert_chain_event_by_block_hash(block_hash, node_id)
    await apply_create_event(raw_db, main_db, block_hash, insert_event['block_number'], insert_event)
    return await main_db.get_block_by_hash(block_hash)


@timeit("[RAW DB] Sync block")
async def sync_block(
        raw_db: RawDB,
        main_db: MainDB,
        block_hash: str,
        block_number: Optional[int] = None,
        is_forked: bool = False,
        chain_event: Optional[Dict[str, Any]] = None,
        rewrite: Optional[bool] = False,
) -> Optional[Dict[str, Any]]:
    data = await load_block_from_raw_db(
        raw_db=raw_db,
        block_hash=block_hash,
        block_number=block_number,
        is_forked=is_forked
    )
    if data:
        block = await process_block(main_db, data)
        await main_db.write_block(chain_event, block, rewrite)
        return block.block

    return None


@timeit("[RAW DB] Load block data")
async def load_block_from_raw_db(
        raw_db: RawDB,
        block_hash: str,
        is_forked: bool,
        block_number: Optional[int] = None
) -> Optional[RawBlockData]:
    receipts = await raw_db.get_block_receipts(block_hash)
    if receipts is None:
        logger.debug("Block is not ready, no receipts", extra={'hash': block_hash})
        return None

    reward = await raw_db.get_reward(block_number, block_hash)
    if reward is None:
        logger.debug("Block is not ready, no reward", extra={'hash': block_hash})
        return None

    return RawBlockData(
        reward=reward,
        receipts=receipts,
        header=await raw_db.get_header_by_hash(block_hash),
        body=await raw_db.get_block_body(block_hash),
        accounts=await raw_db.get_block_accounts(block_hash),
        internal_txs=await raw_db.get_internal_transactions(block_hash),
        token_balances=await raw_db.get_token_holder_balances(block_hash),
        token_descriptions=await raw_db.get_token_descriptions(block_hash),
        is_forked=is_forked
    )


async def apply_create_event(
        raw_db: RawDB,
        main_db: MainDB,
        block_hash: str,
        block_num: int,
        chain_event: Dict[str, Any]
) -> None:
    parent_hash = await raw_db.get_parent_hash(block_hash)
    parent = await main_db.get_block_by_hash(parent_hash)

    is_block_number_exists = await main_db.is_block_number_exists(block_num)
    is_parent_does_not_match_to_canonical_chain = parent and parent['is_forked']
    is_forked = bool(is_block_number_exists or is_parent_does_not_match_to_canonical_chain)

    is_block_exist = await main_db.get_block_by_hash(block_hash) is not None
    if is_block_exist:
        logger.debug(
            "Block already exists, skip and save event...",
            extra={
                'hash': block_hash,
                'event_id': chain_event['id']
            }
        )
        await main_db.insert_chain_event(event=chain_event)
    else:
        await sync_block(
            raw_db=raw_db,
            main_db=main_db,
            block_hash=block_hash,
            block_number=block_num,
            is_forked=is_forked,
            chain_event=chain_event,
        )


async def apply_split_event(main_db: MainDB, split_data: Dict[str, Any], load_missed=None) -> None:
    from_block, to_block = get_split_range(split_data)
    hash_map = await main_db.get_hash_map_from_block_range(from_block, to_block)

    new_chain_fragment = await get_block_chain_from_split(hash_map, split_data, load_missed, direction='add')
    old_chain_fragment = await get_block_chain_from_split(hash_map, split_data, load_missed, direction='drop')

    await main_db.apply_chain_split(
        new_chain_fragment=new_chain_fragment,
        old_chain_fragment=old_chain_fragment,
        chain_event=split_data,
    )
