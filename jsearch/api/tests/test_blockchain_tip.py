import pytest

from jsearch.api.blockchain_tip import is_tip_stale
from jsearch.api.structs import BlockchainTip


async def test_get_tip_or_raise_api_error() -> None:
    pass


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
        "not in fork + historical = not stale"
        "in fork + historical = not stale"
        "in fork + not historical = stale"
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


def test_is_tip_stale_no_tip_provided() -> None:
    assert is_tip_stale(tip=None, block_number=10) is False
