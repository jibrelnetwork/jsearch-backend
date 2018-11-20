import json
from datetime import datetime
from pathlib import Path

import attr
import pytest

RESOURCE_FOLDER = Path(__file__).parent / "resources"


@attr.s
class ContractSource:
    abi: str = attr.ib(default="")
    bin: str = attr.ib(default="")
    sources: str = attr.ib(default="")

    def abi_as_dict(self):
        return json.loads(self.abi)


class FuckTokenSource(ContractSource):
    def as_db_record(self):
        from jsearch.common.contracts import ERC20_ABI
        return {
            'name': 'FucksToken',
            'byte_code': self.bin,
            'source_code': self.sources,
            'abi': ERC20_ABI,
            'compiler_version': 'v0.4.18+commit.9cf6e910',
            'optimization_enabled': True,
            'optimization_runs': 200,
            'constructor_args': '',
            'metadata_hash': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaa55d',
            'is_erc20_token': True,
            'token_name': 'FucksToken',
            'token_symbol': 'ZFUCK',
            'token_decimals': 2,
            'token_total_supply': 1000000000,
            'grabbed_at': None,
            'verified_at': datetime(2018, 5, 10),
        }

    @classmethod
    def load(cls):
        abi: Path = RESOURCE_FOLDER / "FucksToken.abi"
        bin: Path = RESOURCE_FOLDER / "FucksToken.bin"
        source: Path = RESOURCE_FOLDER / "FucksToken.sol"

        return ContractSource(
            abi=abi.read_text(),
            bin=bin.read_text(),
            sources=source.read_text(),
        )


@pytest.fixture(scope="session")
def fuck_token():
    return FuckTokenSource.load()
