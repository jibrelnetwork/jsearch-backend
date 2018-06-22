import copy

from jsearch.common.contracts import ERC20_ABI, simplify_abi, is_erc20_compatible, is_erc20_stricte


"""
ERC_20 items:

'name',
'approve',
'totalSupply',
'transferFrom',
'decimals',
'balanceOf',
'symbol',
'transfer',
'allowance',
None,
'Approval',
'Transfer'
"""


def test_simplify_abi():
    abi_simple = simplify_abi(ERC20_ABI)

    assert abi_simple == [
        {'type': 'function', 'name': 'name', 'inputs': [], 'outputs': ['string']},
        {'type': 'function', 'name': 'approve', 'inputs': ['address', 'uint'], 'outputs': ['bool']},
        {'type': 'function', 'name': 'totalSupply', 'inputs': [], 'outputs': ['uint']},
        {'type': 'function', 'name': 'transferFrom', 'inputs': ['address', 'address', 'uint'], 'outputs': ['bool']},
        {'type': 'function', 'name': 'decimals', 'inputs': [], 'outputs': ['uint']},
        {'type': 'function', 'name': 'balanceOf', 'inputs': ['address'], 'outputs': ['uint']},
        {'type': 'function', 'name': 'symbol', 'inputs': [], 'outputs': ['string']},
        {'type': 'function', 'name': 'transfer', 'inputs': ['address', 'uint']},
        {'type': 'function', 'name': 'allowance', 'inputs': ['address', 'address'], 'outputs': ['uint']},

        {'type': 'event', 'name': 'Approval', 'inputs': ['address', 'address', 'uint']},
        {'type': 'event', 'name': 'Transfer', 'inputs': ['address', 'address', 'uint']},
    ]


def test_is_erc20_compatible_true():
    assert True is is_erc20_compatible(ERC20_ABI)
    abi = copy.deepcopy(ERC20_ABI)
    abi[0].pop('constant')
    abi[7]['inputs'][0]['name'] = 'foo'
    abi[7]['inputs'][1]['name'] = 'bar'
    assert True is is_erc20_compatible(abi)
    abi[1]['inputs'] = [{'type': 'string', 'name': 'var'}]
    assert True is is_erc20_compatible(abi)


def test_is_erc20_compatible_false():
    abi = copy.deepcopy(ERC20_ABI)
    # change transfer description:
    abi[7]['inputs'] = [{'type': 'string', 'name': 'var'}]
    assert False is is_erc20_compatible(abi)


def test_is_erc20_strict_true():
    assert True is is_erc20_stricte(ERC20_ABI)


def test_is_erc20_strict_false():
    abi = copy.deepcopy(ERC20_ABI)
    abi[0].pop('constant')
    abi[1]['inputs'][0]['name'] = 'foo'
    abi[1]['inputs'][1]['name'] = 'bar'
    assert False is is_erc20_stricte(abi)
