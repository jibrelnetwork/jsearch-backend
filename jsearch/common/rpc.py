import logging
from functools import partial
from pprint import pformat
from random import randint
from typing import Any, Dict, List, Optional

import backoff
import requests
import web3
from aiohttp import request
from eth_abi import decode_abi
from eth_abi import encode_abi as eth_abi_encode_abi
from eth_abi.exceptions import DecodingError, EncodingError
from eth_utils import to_hex
from hexbytes import HexBytes
from web3.exceptions import BadFunctionCallOutput
from web3.utils.abi import get_abi_output_types, map_abi_data, get_abi_input_types
from web3.utils.contracts import find_matching_fn_abi, get_function_info
from web3.utils.normalizers import BASE_RETURN_NORMALIZERS, abi_bytes_to_bytes, abi_address_to_hex, abi_string_to_text

from jsearch import settings
from jsearch.typing import Abi

log = logging.getLogger(__name__)

post = partial(request, 'POST')


class EthRequestException(Exception):
    pass


class EthCallException(Exception):
    pass


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


def decode_erc20_output_value(data: Dict[str, Any], fn_identifier: str, abi: Abi, args: Any, kwargs: Any) -> Any:
    function_abi = find_matching_fn_abi(abi, fn_identifier, args, kwargs)
    output_types = get_abi_output_types(function_abi)
    try:
        output_data = decode_abi(output_types, data)
    except DecodingError as e:
        # Provide a more helpful error message than the one provided by
        # eth-abi-utils
        msg = (
            "Could not decode contract function call {} return data {} for "
            "output_types {}".format(
                fn_identifier,
                data,
                output_types
            )
        )
        raise BadFunctionCallOutput(msg) from e

    _normalizers = BASE_RETURN_NORMALIZERS
    normalized_data = map_abi_data(_normalizers, output_types, output_data)

    if len(normalized_data) == 1:
        return normalized_data[0]
    else:
        return normalized_data


class ContractCall:
    __slots__ = (
        'pk',
        'abi',
        'address',
        'method',
        'args',
        'kwargs',
        'block',
        'silent'
    )

    def __init__(self, abi: Abi,
                 address: str,
                 method: str,
                 pk: Optional[int] = None,
                 args: Any = None,
                 kwargs: Any = None,
                 block: Optional[str] = None,
                 silent: bool = False) -> None:
        self.pk = pk if pk is not None else randint(1, 100)
        self.abi = abi
        self.address = address
        self.method = method
        self.args = args
        self.kwargs = kwargs
        self.block = block or 'latest'
        self.silent = silent

    def encode(self) -> Dict[str, Any]:
        try:
            fn_abi, fn_selector, fn_arguments = get_function_info(
                abi=self.abi,
                fn_name=self.method,
                args=self.args,
                kwargs=self.kwargs
            )
            data = HexBytes(fn_selector)
            if fn_arguments:
                data += encode_abi(abi=fn_abi, arguments=fn_arguments)
            data = to_hex(data)
            return {
                "jsonrpc": "2.0",
                "method": 'eth_call',
                "params": [
                    {
                        "to": self.address,
                        "data": data
                    },
                    self.block
                ],
                "id": self.pk,
            }
        except web3.exceptions.ValidationError:
            if not self.silent:
                raise
            logging.error('[JSON RPC] Contract %s does not contain method %s', self.address, self.method)

    def decode(self, value) -> Any:
        try:
            return decode_erc20_output_value(
                data=value,
                fn_identifier=self.method,
                abi=self.abi,
                args=self.args,
                kwargs=self.kwargs
            )
        except BadFunctionCallOutput:
            logging.error('[JSON RPC] Contract %s was destroyed and cannot by read at last block', self.address)
        except web3.exceptions.ValidationError:
            if not self.silent:
                raise
            logging.error('[JSON RPC] Contract %s does not contain method %s', self.address, self.method)


ContractCalls = List[ContractCall]


@backoff.on_exception(
    backoff.fibo,
    max_tries=10,
    exception=(EthRequestException, requests.exceptions.RequestException)
)
def eth_call_request(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    response = requests.post(url=settings.ETH_NODE_URL, json=data)
    if response.status_code != 200:
        raise EthRequestException(f"[REQUEST] {settings.ETH_NODE_URL}: {response.status_code}, {response.reason}")

    data = response.json()
    if any('error' in item for item in data):
        msg = pformat(data)
        raise EthCallException(
            f"[REQUEST] {settings.ETH_NODE_URL}: "
            f"{response.status_code}, {response.reason}: {msg}"
        )

    return data


def eth_call(call: ContractCall) -> Any:
    data = [call.encode()]
    response = eth_call_request(data)
    results = {result["id"]: HexBytes(result["result"]) for result in response}
    return call.decode(results[call.pk])


def eth_call_batch(calls: ContractCalls) -> Dict[str, Any]:
    data = [call.encode() for call in calls]
    data = [item for item in data if item]

    response = eth_call_request(data)
    results = {result["id"]: HexBytes(result["result"]) for result in response}

    return {call.pk: call.decode(results[call.pk]) for call in calls if call.pk in results}
