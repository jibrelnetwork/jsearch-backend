import binascii
import logging

from ethereum.abi import ContractTranslator
from ethereum.utils import decode_hex

from jsearch.typing import Abi_ERC20, AccountAddress

logger = logging.getLogger(__name__)

NULL_ADDRESS: AccountAddress = '0x0000000000000000000000000000000000000000'

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

ERC20_DEFAULT_DECIMALS = 18
ERC20_ABI: Abi_ERC20 = [
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

ERC20_ABI_SIMPLE = [
    {'type': 'function', 'name': 'transfer', 'inputs': ['address', 'uint']},
    {'type': 'function', 'name': 'balanceOf', 'inputs': ['address'], 'outputs': ['uint']},
    {'type': 'function', 'name': 'decimals', 'inputs': [], 'outputs': ['uint']},
    {'type': 'function', 'name': 'totalSupply', 'inputs': [], 'outputs': ['uint']},

    {'type': 'event', 'name': 'Transfer', 'inputs': ['address', 'address', 'uint']},
]


def _fix_string_args(args, types):
    fixed = []
    for i, arg in enumerate(args):
        t = types[i]
        arg = _fix_arg(arg, t)
        fixed.append(arg)
    return fixed


def _fix_arg(arg, typ, decoder=None):
    if not decoder:
        if typ == 'string' and isinstance(arg, bytes):
            def decoder(a):
                return a.decode().replace('\x00', '')
        elif typ.startswith('byte'):
            def decoder(a):
                return binascii.hexlify(a).decode()  # FIXME! handle bytes properly
        else:
            def decoder(a):
                return a

    if isinstance(arg, list):
        return [_fix_arg(a, typ, decoder) for a in arg]
    return decoder(arg)


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
