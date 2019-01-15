from functools import partial
from typing import List

from funcy import cached_property
from web3 import Web3

from jsearch.common.rpc import ContractCall, eth_call


class TokenProxy:

    def __init__(self, abi, address):
        self.abi = abi
        self.address = address
        self.address_as_checksum = Web3.toChecksumAddress(self.address)

        self.call = partial(ContractCall, abi=self.abi, address=self.address_as_checksum)

    @cached_property
    def decimals(self):
        return eth_call(self.call(method='decimals'))

    @cached_property
    def total_supply(self):
        return eth_call(self.call(method='totalSupply'))

    def get_balance_call(self, pk: int, args: List[str]):
        return self.call(method='balanceOf', pk=pk, args=args)
