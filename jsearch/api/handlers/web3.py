
from ethereum.utils import decode_hex, encode_hex
from Crypto.Hash import keccak

from jsearch.api.error_code import ErrorCode
from jsearch.api.helpers import api_success, api_error_response_400, load_json_or_raise_api_error, ApiError


@ApiError.catch
async def sha3(request):
    args = await load_json_or_raise_api_error(request)

    if 'data' not in args:
        return api_error_response_400(errors=[
            {
                'field': 'data',
                'code': ErrorCode.PARAM_REQUIRED,
                'message': f'The data field does not exist'
            }
        ])

    try:
        decoded_data = decode_hex(args['data'].replace('0x', ''))
        result = f'0x{encode_hex(keccak.new(digest_bits=256, data=decoded_data).digest())}'

        return api_success(data=result)
    except (TypeError, ValueError, AttributeError):
        return api_error_response_400(errors=[
            {
                'field': 'data',
                'code': ErrorCode.INVALID_VALUE,
                'message': f'Cannot unmarshal hex string'
            }
        ])
