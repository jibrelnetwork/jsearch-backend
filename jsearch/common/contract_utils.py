import binascii

from ethereum.abi import (
    decode_abi,
    normalize_name as normalize_abi_method_name,
    method_id as get_abi_method_id,
    ContractTranslator)
from ethereum.utils import encode_int, zpad, decode_hex


ERC20_METHODS_IDS = {
    'approve': '0x095ea7b3',
    'transfer': '0xa9059cbb',
    'transferFrom': '0x23b872dd',

    'balanceOf': '0x70a08231',
    'allowance': '0xdd62ed3e',

    'name': '0x06fdde03',
    'symbol': '0x95d89b41',
    'decimals': '0x313ce567',
    'totalSupply': '0x18160ddd',

}


ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": False,
        "inputs": [
            {
                "name": "_spender",
                "type": "address"
            },
            {
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "approve",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": False,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "totalSupply",
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": False,
        "inputs": [
            {
                "name": "_from",
                "type": "address"
            },
            {
                "name": "_to",
                "type": "address"
            },
            {
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "transferFrom",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": False,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [
            {
                "name": "",
                "type": "uint8"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {
                "name": "_owner",
                "type": "address"
            }
        ],
        "name": "balanceOf",
        "outputs": [
            {
                "name": "balance",
                "type": "uint256"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": False,
        "inputs": [
            {
                "name": "_to",
                "type": "address"
            },
            {
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "transfer",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": False,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {
                "name": "_owner",
                "type": "address"
            },
            {
                "name": "_spender",
                "type": "address"
            }
        ],
        "name": "allowance",
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "payable": True,
        "stateMutability": "payable",
        "type": "fallback"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "name": "owner",
                "type": "address"
            },
            {
                "indexed": True,
                "name": "spender",
                "type": "address"
            },
            {
                "indexed": False,
                "name": "value",
                "type": "uint256"
            }
        ],
        "name": "Approval",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "name": "from",
                "type": "address"
            },
            {
                "indexed": True,
                "name": "to",
                "type": "address"
            },
            {
                "indexed": False,
                "name": "value",
                "type": "uint256"
            }
        ],
        "name": "Transfer",
        "type": "event"
    }
]


def _fix_string_args(args, types):
    # print('FSA', args, types)
    fixed = []
    for i, arg in enumerate(args):
        t = types[i]
        arg = _fix_arg(arg, t)
        fixed.append(arg)
    return fixed


def _fix_arg(arg, typ, decoder=None):
    if not decoder:
        if typ == 'string' and isinstance(arg, bytes):
            decoder = lambda a: a.decode().replace('\x00', '')
        elif typ.startswith('byte'):
            decoder = lambda a: binascii.hexlify(a).decode()  # FIXME! handle bytes properly
        else:
            decoder = lambda a: a

    if isinstance(arg, list):
        return [_fix_arg(a, typ, decoder) for a in arg]
    return decoder(arg)


def decode_contract_call(contract_abi: list, call_data: str):
    call_data = call_data.replace('0x', '')
    call_data_bin = decode_hex(call_data)
    method_signature = call_data_bin[:4]
    for description in contract_abi:
        if description.get('type') != 'function':
            continue
        method_name = normalize_abi_method_name(description['name'])
        arg_types = [item['type'] for item in description['inputs']]
        method_id = get_abi_method_id(method_name, arg_types)
        if zpad(encode_int(method_id), 4) == method_signature:
            try:
                args = decode_abi(arg_types, call_data_bin[4:])
                args = _fix_string_args(args, arg_types)
            except AssertionError as e:
                continue
            return {'function': method_name, 'args': args}


def decode_event(contract_abi, event):
    ct = ContractTranslator(contract_abi)
    log_topics = [int(t[2:], 16) for t in event['topics']]
    log_data = decode_hex(event['data'].replace('0x', ''))
    decoded = ct.decode_event(log_topics, log_data)
    event_type = decoded.pop('_event_type').decode()
    eabi = [e for e in contract_abi if e['type'] == 'event' and e['name'] == event_type][0]
    args_map = {i['name']: i['type'] for i in eabi['inputs']}
    fixed_event = {'_event_type': event_type}
    for k, v in decoded.items():
        t = args_map[k]
        fixed_event[k] = _fix_arg(v, t)
    return fixed_event


def collect_types():
    from jsearch.common.tables import contracts_t
    from sqlalchemy.sql import select
    from sqlalchemy import create_engine
    import json

    engine = create_engine('postgresql://dbuser@localhost/jsearch_main')
    conn = engine.connect()
    types = set()
    s = select([contracts_t])
    rows = conn.execute(s)

    for row in rows:
        print(row['address'])
        # abi = json.loads(row['abi'])
        abi = row['abi']
        for t in abi:
            if t['type'] in ('function', 'event'):
                for i in t['inputs']:
                    types.add(i['type'])
    return types
