import logging

import aiohttp

from jsearch import settings
from jsearch.api.helpers import api_success
from jsearch.common.contracts import cut_contract_metadata_hash, is_erc20_compatible

logger = logging.getLogger(__name__)


async def verify_contract(request):
    """
    address
    contract_name
    compiler
    optimization_enabled
    constructor_args
    source_code
    """

    input_data = await request.json()
    constructor_args = input_data.pop('constructor_args') or ''
    address = input_data.pop('address')

    contract_creation_code = await request.app['storage'].get_contact_creation_code(address)

    async with aiohttp.request('POST', settings.JSEARCH_COMPILER_API + '/v1/compile', json=input_data) as resp:
        res = await resp.json()

    byte_code = res['bin']
    byte_code, _ = cut_contract_metadata_hash(byte_code)
    bc_byte_code, mhash = cut_contract_metadata_hash(contract_creation_code)

    if byte_code + constructor_args == bc_byte_code.replace('0x', ''):
        verification_passed = True
        is_erc20_token = is_erc20_compatible(res['abi'])
        contract_data = dict(
            address=address,
            contract_creation_code=contract_creation_code,
            mhash=mhash,
            abi=res['abi'],
            constructor_args=constructor_args,
            is_erc20_token=is_erc20_token,
            **input_data
        )
        async with aiohttp.request('POST', settings.JSEARCH_CONTRACTS_API + '/v1/contracts',
                                   json=contract_data) as resp:
            await resp.json()
    else:
        verification_passed = False
    return api_success({'verification_passed': verification_passed})
