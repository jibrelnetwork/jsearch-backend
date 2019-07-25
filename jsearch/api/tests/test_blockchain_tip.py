import pytest

from jsearch.api.blockchain_tip import is_tip_stale, get_tip_or_raise_api_error
from jsearch.api.helpers import ApiError
from jsearch.api.structs import BlockchainTip, BlockInfo


async def test_get_tip_or_raise_api_error_raises_an_error_if_there_s_no_tip_block(storage) -> None:
    with pytest.raises(ApiError) as exc:
        await get_tip_or_raise_api_error(storage, '0x1')

    assert exc.value.errors == [
        {
            'field': 'tip',
            'error_code': 'BLOCK_NOT_FOUND',
            'error_message': 'Block with hash 0x1 not found'
        }
    ]


@pytest.mark.parametrize('is_tip_forked', (True, False))
async def test_get_tip_or_raise_api_error_finds_tip_not_forked(
        is_tip_forked,
        storage,
        block_factory,
        chain_split_factory,
        reorg_factory
) -> None:
    """

    --[15]---[16]-- Canonical branch
          \
           \
            -[16]-- Forked branch

    """
    common_block = block_factory.create(number=15, hash='0x111')

    canonical_block = block_factory.create(parent_hash=common_block.hash, number=16, hash='0x222a')
    forked_block = block_factory.create(parent_hash=common_block.hash, number=16, hash='0x222b')

    chain_splits = chain_split_factory.create(
        common_block_hash=common_block.hash,
        common_block_number=common_block.number,
    )
    reorg_factory.create(
        block_hash=forked_block.hash,
        block_number=forked_block.number,
        split_id=chain_splits.id,
    )

    tip_block = forked_block if is_tip_forked else canonical_block
    tip = await get_tip_or_raise_api_error(storage, tip_block.hash)

    assert {
        'tip_hash': tip_block.hash,
        'tip_number': tip_block.number,
        'last_hash': '0x222a',
        'last_number': 16,
        'is_in_fork': is_tip_forked,
        'last_unchanged_block': 15 if is_tip_forked else None,
    } == {
        'tip_hash': tip.tip_hash,
        'tip_number': tip.tip_number,
        'last_hash': tip.last_hash,
        'last_number': tip.last_number,
        'is_in_fork': tip.is_in_fork,
        'last_unchanged_block': tip.last_unchanged_block,
    }


async def test_get_tip_or_raise_api_error_finds_tip_last_block_provided(
        storage,
        block_factory,
        chain_split_factory,
        reorg_factory
) -> None:
    """

    --[14]---[15]---[16]---[17]-- Canonical branch
                 \
                  \
                   -[16]-- Forked branch

    """
    common_block = block_factory.create(number=15, hash='0x111')

    canonical_block = block_factory.create(parent_hash=common_block.hash, number=16, hash='0x222a')
    forked_block = block_factory.create(parent_hash=common_block.hash, number=16, hash='0x222b')

    chain_splits = chain_split_factory.create(
        common_block_hash=common_block.hash,
        common_block_number=common_block.number,
    )
    reorg_factory.create(
        block_hash=forked_block.hash,
        block_number=forked_block.number,
        split_id=chain_splits.id,
    )

    tip = await get_tip_or_raise_api_error(
        storage,
        canonical_block.hash,
        last_block=BlockInfo(hash='0xLASTBLOCK', number=42)
    )

    assert {
        'tip_hash': '0x222a',
        'tip_number': 16,
        'last_hash': '0xLASTBLOCK',
        'last_number': 42,
        'is_in_fork': False,
        'last_unchanged_block': None,
    } == {
        'tip_hash': tip.tip_hash,
        'tip_number': tip.tip_number,
        'last_hash': tip.last_hash,
        'last_number': tip.last_number,
        'is_in_fork': tip.is_in_fork,
        'last_unchanged_block': tip.last_unchanged_block,
    }


@pytest.mark.parametrize(
    "is_in_fork, is_historical_data, is_stale",
    [
        (
            False,
            False,
            False,
        ),
        (
            False,
            True,
            False,
        ),
        (
            True,
            True,
            False,
        ),
        (
            True,
            False,
            True,
        ),
    ],
    ids=[
        "not in fork + not historical = not stale",
        "not in fork + historical = not stale",
        "in fork + historical = not stale",
        "in fork + not historical = stale",
    ]
)
def test_is_tip_stale(is_in_fork, is_historical_data, is_stale) -> None:
    """

    --[15]---[16]---[17]---[18]---[19]---[20]---[21]-- Canonical last branch
          \
           \
            -[16]---[17]---[18]---[19]---[20]-- Forked tip branch

    """

    tip = BlockchainTip(
        tip_hash='0x1',
        tip_number=20,
        last_hash='0x2',
        last_number=21,
        is_in_fork=is_in_fork,
        last_unchanged_block=15 if is_in_fork else None,
    )

    block_to_check = 15 if is_historical_data else 20

    assert is_tip_stale(tip, block_to_check) is is_stale


def test_is_tip_stale_no_block_number_provided() -> None:
    tip = BlockchainTip(
        tip_hash='0x1',
        tip_number=20,
        last_hash='0x2',
        last_number=21,
        is_in_fork=False,
        last_unchanged_block=None,
    )

    assert is_tip_stale(tip=tip, block_number=None) is False
