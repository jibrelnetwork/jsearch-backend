import factory
from factory import DictFactory

from jsearch.tests.plugins.databases.factories.common import generate_address


class RawDBReceiptsFactory(DictFactory):
    block_number = factory.sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)

    @factory.lazy_attribute
    def fields(self):
        return {
            "Receipts": [
                {
                    "logs": [
                        {
                            "data": "0x000000000000000000000000000000000000000000000000000000001954fc40",
                            "topics": [
                                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                                "0x0000000000000000000000002819c144d5946404c0516b6f817a960db37d4929",
                                "0x0000000000000000000000002611568cf62d123f2c91699eb5ad665b2d98a6fa"
                            ],
                            "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                            "removed": False,
                            "logIndex": "0x0",
                            "blockHash": self.block_hash,
                            "blockNumber": str(hex(self.block_number)),
                            "transactionHash": "0x12cf6c38da4b649b75a4d2aa1f5c77ac7ff980d8506f7f0b68fb2246fba1725e",
                            "transactionIndex": "0x0"
                        }
                    ],
                    "root": "0x",
                    "status": "0x1",
                    "gasUsed": "0xd0d9",
                    "blockHash": "0xf6ea9ba586cefe98db0f581519947ddac5ce720da91e2dbf3a684b02444d6181",
                    "logsBloom": "0x000000000000000000000000000000000000000000000000000000000000000000"
                                 "00000000000000000000000000010000000000000000000000000000002000000000"
                                 "00000000000000000800000000000000000000000000000000000000000000000000"
                                 "00000000000000000000000000000000000000000000100000000000000000000000"
                                 "00000000000000100000000000000000000040000000100000000000000000000000"
                                 "00008000000000000000000000000000000000000000000000000200000000000000"
                                 "00000000100000000000000000000000000000000000000008000000000000000000"
                                 "00000000000000000080000000000000000000",
                    "blockNumber": "0x81b342",
                    "contractAddress": "0x0000000000000000000000000000000000000000",
                    "transactionHash": "0x12cf6c38da4b649b75a4d2aa1f5c77ac7ff980d8506f7f0b68fb2246fba1725e",
                    "transactionIndex": "0x0",
                    "cumulativeGasUsed": "0xd0d9"
                },
            ]
        }
