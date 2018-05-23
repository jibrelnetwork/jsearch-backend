import copy

from jsearch.common.contracts import ERC20_ABI, simplify_abi, is_erc20_compatible, is_erc20_stricte


def test_simplify_abi():
    abi_simple = simplify_abi(ERC20_ABI)

    assert abi_simple == [
        {'type': 'function', 'name': 'name', 'inputs': [], 'outputs': ['string']},
        {'type': 'function', 'name': 'approve', 'inputs': ['address', 'uint256'], 'outputs': ['bool']},
        {'type': 'function', 'name': 'totalSupply', 'inputs': [], 'outputs': ['uint256']},
        {'type': 'function', 'name': 'transferFrom', 'inputs': ['address', 'address', 'uint256'], 'outputs': ['bool']},
        {'type': 'function', 'name': 'decimals', 'inputs': [], 'outputs': ['uint8']},
        {'type': 'function', 'name': 'balanceOf', 'inputs': ['address'], 'outputs': ['uint256']},
        {'type': 'function', 'name': 'symbol', 'inputs': [], 'outputs': ['string']},
        {'type': 'function', 'name': 'transfer', 'inputs': ['address', 'uint256'], 'outputs': ['bool']},
        {'type': 'function', 'name': 'allowance', 'inputs': ['address', 'address'], 'outputs': ['uint256']},

        {'type': 'event', 'name': 'Approval', 'inputs': ['address', 'address', 'uint256']},
        {'type': 'event', 'name': 'Transfer', 'inputs': ['address', 'address', 'uint256']},
    ]


def test_is_erc20_compatible_true():
    assert True is is_erc20_compatible(ERC20_ABI)
    abi = copy.deepcopy(ERC20_ABI)
    abi[0].pop('constant')
    abi[1]['inputs'][0]['name'] = 'foo'
    abi[1]['inputs'][1]['name'] = 'bar'
    assert True is is_erc20_compatible(abi)


def test_is_erc20_compatible_false():
    abi = copy.deepcopy(ERC20_ABI)
    abi[0]['inputs'] = [{'type': 'string', 'name': 'var'}]
    assert False is is_erc20_compatible(abi)


def test_is_erc20_strict_true():
    assert True is is_erc20_stricte(ERC20_ABI)


def test_is_erc20_strict_false():
    abi = copy.deepcopy(ERC20_ABI)
    abi[0].pop('constant')
    abi[1]['inputs'][0]['name'] = 'foo'
    abi[1]['inputs'][1]['name'] = 'bar'
    assert False is is_erc20_stricte(abi)
