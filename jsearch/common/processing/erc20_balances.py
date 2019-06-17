import asyncio
import logging
from pprint import pformat

import aiohttp
import time
from eth_abi import encode_abi as eth_abi_encode_abi
from eth_abi.exceptions import EncodingError
from eth_utils import to_hex
from hexbytes import HexBytes
from typing import List, Optional
from web3 import Web3
from web3.utils.abi import map_abi_data, get_abi_input_types
from web3.utils.normalizers import abi_bytes_to_bytes, abi_address_to_hex, abi_string_to_text

from jsearch.common.last_block import LastBlock
from jsearch.common.rpc import EthRequestException, EthCallException

logger = logging.getLogger(__name__)


class BalanceUpdate:
    token_address: str
    account_address: str
    block: int

    value: Optional[int]
    decimals: Optional[int]

    __slots__ = (
        'abi',
        'token_address',
        'account_address',
        'block',
        'decimals',
        'value'
    )

    def __init__(self, token_address, account_address, block, abi, decimals):
        self.token_address = token_address
        self.account_address = account_address
        self.block = block
        self.abi = abi
        self.value = None
        self.decimals = decimals

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        if not isinstance(other, BalanceUpdate):
            raise ValueError('Expected BalanceUpdate instance')
        return self.key == other.key

    @property
    def token_as_checksum(self):
        return Web3.toChecksumAddress(self.token_address)

    @property
    def account_as_checksum(self):
        return Web3.toChecksumAddress(self.account_address)

    @property
    def key(self):
        return self.token_address, self.account_address

    def apply(self, db, last_block: int):
        changes = None
        balance = None

        is_valid = isinstance(self.value, int)
        if is_valid:

            if last_block != LastBlock.LATEST_BLOCK:
                changes = db.get_balance_changes_since_block(
                    token=self.token_address,
                    account=self.account_address,
                    block_number=last_block
                )
            else:
                changes = 0

            balance = self.value + changes
            if is_valid:
                db.update_token_holder_balance(self.token_address, self.account_address, balance, self.decimals)
                logger.info(
                    'Updated balance for an account',
                    extra={
                        'tag': 'BALANCE UPDATE',
                        'block': self.block,
                        'last_block': last_block,
                        'token_address': self.token_address,
                        'account_address': self.account_address,
                        'before_update': self.value,
                        'delta': changes,
                        'after_update': balance,
                    }
                )

        if not is_valid:
            logger.error(
                'Failed to update balance for an account',
                extra={
                    'tag': 'BALANCE UPDATE',
                    'block': self.block,
                    'last_block': last_block,
                    'token_address': self.token_address,
                    'account_address': self.account_address,
                    'before_update': self.value,
                    'delta': changes,
                    'after_update': balance,
                }
            )

    def to_asset_update(self):
        decimals = self.decimals or 18
        return {
            'asset_address': self.token_address,
            'address': self.account_address,
            'value': self.value,
            'decimals': decimals
        }


BalanceUpdates = List[BalanceUpdate]

abi = {
    'constant': True,
    'inputs': [
        {
            'name': '_owner',
            'type': 'address'
        }
    ],
    'name': 'balanceOf',
    'outputs': [
        {
            'name': 'balance',
            'type': 'uint256'
        }
    ],
    'payable': False,
    'stateMutability': 'view',
    'type': 'function'
}


def encode_abi(abi, arguments):
    argument_types = get_abi_input_types(abi)
    try:
        normalizers = [
            abi_address_to_hex,
            abi_bytes_to_bytes,
            abi_string_to_text,
        ]
        normalized_arguments = map_abi_data(
            normalizers,
            argument_types,
            arguments,
        )
        encoded_arguments = eth_abi_encode_abi(
            argument_types,
            normalized_arguments,
        )
    except EncodingError as e:
        raise TypeError(
            "One or more arguments could not be encoded to the necessary "
            "ABI type: {0}".format(str(e))
        )

    return encoded_arguments


def get_data(owner):
    address = Web3.toChecksumAddress(owner)
    data = to_hex(HexBytes('0x70a08231') + encode_abi(abi, [address]))
    return data


def get_params(contract, data):
    return [
        {
            "to": contract,
            "data": data
        },
        "latest"
    ]


def get_params_balance(contract, owner):
    return get_params(contract, get_data(owner))


def get_balance_rpc_call(contract, owner, _id):
    value = {
        "jsonrpc": "2.0",
        "method": 'eth_call',
        "params": get_params(contract, get_data(owner)),
        "id": _id,
    }
    return value


def get_decimals_rpc_call(contract, _id):
    value = {
        "jsonrpc": "2.0",
        "method": 'eth_call',
        "params": get_params(contract, '0x313ce5670000000000000000000000'),
        "id": _id,
    }
    return value


API_URL = 'https://main-node.jwallet.network'


async def eth_call_request(data):
    rs = time.time()
    async with aiohttp.ClientSession() as session:
        async with session.post(API_URL, json=data) as response:
            if response.status != 200:
                raise EthRequestException(
                    f"[REQUEST] {settings.ETH_NODE_URL}: {response.status_code}, {response.reason}"
                )
            data = await response.json()
            print('RPC REQ', API_URL, time.time() - rs)
            if any('error' in item for item in data):
                msg = pformat(data)
                raise EthCallException(
                    f"[REQUEST] {settings.ETH_NODE_URL}: "
                    f"{response.status}, {response.reason}: {msg}"
                )
            return data


async def eth_call(call):
    data = call
    response = eth_call_request(data)
    results = {result["id"]: HexBytes(result["result"]) for result in response}
    return results


async def eth_call_batch(calls):
    # data = [call.encode() for call in calls]
    data = [item for item in calls if item]

    response = await eth_call_request(data)
    results = {result["id"]: HexBytes(result["result"]) for result in response}

    return results


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


async def get_balances(owners, batch_size):
    calls = []
    gt = time.time()
    for i, h in enumerate(owners):
        call = get_balance_rpc_call(h[1], h[0], i)
        calls.append(call)
    print('RPC GEN TIME', time.time() - gt, len(owners))

    calls_chunks = chunks(calls, batch_size)
    coros = [eth_call_batch(calls=c) for c in calls_chunks]
    calls_results_list = await asyncio.gather(*coros)
    calls_results = {}
    for res in calls_results_list:
        calls_results.update(res)

    print('BALANCEOF TOTAL TIME', time.time() - gt, batch_size)
    balances = []
    for i, h in enumerate(owners):
        hex_val = calls_results[i].hex().replace('0x', '', 1)[:64]
        if hex_val == '':
            balance = 0
        else:
            balance = int(hex_val, 16)
        balances.append((h[0], h[1], balance))
    return balances


async def get_decimals(addresses, batch_size):
    calls = []
    gt = time.time()
    for i, addr in enumerate(addresses):
        call = get_decimals_rpc_call(addr, i)
        calls.append(call)
        calls_chunks = chunks(calls, batch_size)
        # TODO: Think about better implementation

    coros = [eth_call_batch(calls=c) for c in calls_chunks]
    calls_results_list = await asyncio.gather(*coros)
    calls_results = {}
    for res in calls_results_list:
        calls_results.update(res)

    print('DECIMALS TOTAL TIME', time.time() - gt, batch_size)
    decimals = {}
    for i, addr in enumerate(addresses):
        try:
            value = int(to_hex(calls_results[i]), 16)
        except ValueError:
            value = 18
        if value > 255:
            value = 18
        decimals[addr] = value
    return decimals
