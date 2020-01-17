import factory
from factory import DictFactory

from jsearch.tests.plugins.databases.factories.common import generate_address


class RawDBRewardFactory(DictFactory):
    block_number = factory.sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)
    address = factory.LazyFunction(generate_address)

    @factory.lazy_attribute
    def fields(self):
        return {
            "Uncles": [],
            "TimeStamp": 1567821252,
            "TxsReward": 55702217748492438,
            "BlockMiner": "0x829bd824b016326a401d083b33d092293333a830",
            "BlockNumber": self.block_number,
            "BlockReward": 2000000000000000000,
            "UnclesReward": 3500000000000000000,
            "UncleInclusionReward": 125000000000000000
        }
