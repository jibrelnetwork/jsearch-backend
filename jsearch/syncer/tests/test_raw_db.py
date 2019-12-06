import pytest
from aiopg.sa import Engine

from jsearch.syncer.database import RawDB
from jsearch.syncer.structs import TokenHolderBalance


@pytest.mark.asyncio
async def test_raw_db_get_token_holder_balances_does_not_fail_if_balance_is_over_bigint(
        raw_db_wrapper: RawDB,
        raw_db: Engine,
) -> None:
    target_block_hash = '0x0f654d506b99728c5e6a9190b87e9d26ad54ab782feb72cece0b2e90b367d0cf'

    raw_db.execute(
        """
        INSERT INTO token_holders (
          "id",
          "block_number",
          "block_hash",
          "token_address",
          "holder_address",
          "balance",
          "decimals"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, [
            (
                168735761,
                8496879,
                target_block_hash,
                '0x07597255910a51509CA469568B048F2597E72504',
                '0x847F13Aaac8a12fa3405B99Ad4304Bdc93D8382D',
                301748400000000001500000,
                18
            ),
        ]
    )

    balances = await raw_db_wrapper.get_token_holder_balances(target_block_hash)

    assert balances == [
        TokenHolderBalance(
            token='0x07597255910a51509ca469568b048f2597e72504',
            account='0x847f13aaac8a12fa3405b99ad4304bdc93d8382d',
            balance=301748400000000001500000,
            block_hash=target_block_hash,
            block_number=8496879,
            decimals=18
        )
    ]
