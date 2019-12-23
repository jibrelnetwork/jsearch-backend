import factory
from factory import DictFactory

from jsearch.tests.plugins.databases.factories.common import generate_address


class RawDBBodyFactory(DictFactory):
    block_number = factory.sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)
    address = factory.LazyFunction(generate_address)

    @factory.lazy_attribute
    def fields(self):
        return {
            "Uncles": [],
            "Transactions": [
                {
                    "r": "0x36e94867cd870efa68af41dbe2364e8347f4ea55eb1d4d99097aba0bf8977e88",
                    "s": "0x7d928cac28a62664b349f3d6d09fd795caa8957e06965d69058d387dd9724c9f",
                    "v": "0x25",
                    "to": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                    "gas": "0x15f90",
                    "from": "0x2819c144d5946404c0516b6f817a960db37d4929",
                    "hash": "0x12cf6c38da4b649b75a4d2aa1f5c77ac7ff980d8506f7f0b68fb2246fba1725e",
                    "input": "0xa9059cbb0000000000000000000000002611568cf62d123f2c91699eb5ad665b"
                             "2d98a6fa000000000000000000000000000000000000000000000000000000001954fc40",
                    "nonce": "0x2d052",
                    "value": "0x0",
                    "gasPrice": "0x9502f9000"
                },
            ]
        }
