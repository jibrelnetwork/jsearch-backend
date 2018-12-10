import json

from eth_abi import decode_abi
from eth_abi.exceptions import DecodingError
from eth_utils import to_bytes, to_text
from web3 import HTTPProvider
from web3.exceptions import BadFunctionCallOutput
from web3.utils.abi import get_abi_output_types, map_abi_data
from web3.utils.contracts import find_matching_fn_abi
from web3.utils.normalizers import BASE_RETURN_NORMALIZERS

from jsearch.common.contracts import ERC20_ABI
from jsearch.utils import suppress_exception


@suppress_exception
def decode_erc20_output_value(data, fn_identifier, args=None, kwargs=None):
    function_abi = find_matching_fn_abi(ERC20_ABI, fn_identifier, args, kwargs)
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


class BatchHTTPProvider(HTTPProvider):

    def decode_rpc_response(self, responses):
        responses = json.loads(to_text(responses))
        return sorted(responses, key=lambda x: x['id'])

    def encode_rpc_request(self, method, params):
        calls = []
        for call_params in params:
            call = {
                "jsonrpc": "2.0",
                "method": method,
                "params": call_params or [],
                "id": next(self.request_counter),
            }
            calls.append(call)
        return to_bytes(text=json.dumps(calls))
