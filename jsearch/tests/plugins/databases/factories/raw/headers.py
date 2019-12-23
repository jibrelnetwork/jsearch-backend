import factory
from factory import DictFactory

from jsearch.tests.plugins.databases.factories.common import generate_address


class RawDBHeadersFactory(DictFactory):
    block_number = factory.sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)

    @factory.lazy_attribute
    def fields(self):
        return {
            "hash": self.block_hash,
            "number": str(hex(self.block_number)),
            "miner": "0x829bd824b016326a401d083b33d092293333a830",
            "nonce": "0x9767c8f042566708",
            "gasUsed": "0x79ed0b",
            "mixHash": "0x24e96de6daf7a28e181f5a14690f70a8ea6cdb04679e4993bef7e350f4c40281",
            "gasLimit": "0x7a4f0e",
            "extraData": "0x7070796520e4b883e5bda9e7a59ee4bb99e9b1bc4e5ce21c",
            "logsBloom": "0xa04482440048502900040108102a08043020208002100240013009"
                         "4080082001102010042c32800120002c8200200d000a0020004828020b611a139"
                         "00010a800410010009120020110209828001270096200080089080c884a085400"
                         "818000022000100182118220200284021000090000089000080040422000c9940"
                         "2040402090010000a4008a000380000000010d1044008010010880000c00100021"
                         "000680000c10002950310924540a0000000844000006030000000000001801c802"
                         "9000008100250802054008042200400845020000830000031000100880908da270"
                         "00100a00900000020040a00000408008404010002020080488401006000c00001",
            "stateRoot": "0xecd24300ac0c97dd7cc2a8f5594dadcd05171fcc0be01509d6c26e19a4a8b453",
            "timestamp": "0x5d730dc4",
            "difficulty": "0x7de6eed0e4196",
            "parentHash": "0xf162142605cf5bf1ba8519e69663b9e640ad063e5f953992453e0c65cf7355d4",
            "sha3Uncles": "0x2d05a0cb29c11079692b550fec51353d007bdc55b88fd7eb35a450ad943bf65f",
            "receiptsRoot": "0xc325c08d8390fe746e5144d9c29267f5ced9b2c944168218f455d6563a38a69b",
            "transactionsRoot": "0xa36db4ebde0c9b6ff9032f1289d0d88687d2cfa60fc398cff80e556c5d269695"
        }
