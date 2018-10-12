import binascii
import os
import logging
import time
import re

from solc import compile_source, install_solc
import solc.install
from ethereum.abi import (
    decode_abi,
    normalize_name as normalize_abi_method_name,
    method_id as get_abi_method_id,
    ContractTranslator)
from ethereum.utils import encode_int, zpad, decode_hex


logger = logging.getLogger(__name__)


NULL_ADDRESS = '0x0000000000000000000000000000000000000000'


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


ERC20_ABI_SIMPLE = [
    {'type': 'function', 'name': 'transfer', 'inputs': ['address', 'uint']},
    {'type': 'function', 'name': 'balanceOf', 'inputs': ['address'], 'outputs': ['uint']},
    {'type': 'function', 'name': 'decimals', 'inputs': [], 'outputs': ['uint']},
    {'type': 'function', 'name': 'totalSupply', 'inputs': [], 'outputs': ['uint']},

    {'type': 'event', 'name': 'Transfer', 'inputs': ['address', 'address', 'uint']},
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
            def decoder(a): return a.decode().replace('\x00', '')
        elif typ.startswith('byte'):
            def decoder(a): return binascii.hexlify(a).decode()  # FIXME! handle bytes properly
        else:
            def decoder(a): return a

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


def is_erc20_stricte(abi):
    for item in ERC20_ABI:
        if item not in abi:
            return False
    return True


def is_erc20_compatible(abi):
    abi_simple = simplify_abi(abi)
    for item in ERC20_ABI_SIMPLE:
        if item not in abi_simple:
            return False
    return True


def simplify_abi(abi):
    
    def fix_uint(typ):
        if typ.startswith('uint'):
            return 'uint'
        return typ

    abi_simple = []
    for item in abi:
        if item.get('type') not in {'function', 'event'}:
            continue
        s = {}
        s['name'] = item['name']
        s['type'] = item['type']
        s['inputs'] = [fix_uint(v['type']) for v in item['inputs']]
        if 'outputs' in item and item['name'] != 'transfer':  #  dont check outputs for transfer
            s['outputs'] = [fix_uint(v['type']) for v in item['outputs']]
        abi_simple.append(s)
    return abi_simple


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
        abi = row['abi']
        for t in abi:
            if t['type'] in ('function', 'event'):
                for i in t['inputs']:
                    types.add(i['type'])
    return types


def compile_contract(source, contract_name, compiler_version,
                     optimization_enambled, optimizer_runs):
    solc_bin = get_solc_bin_path(compiler_version)
    try:
        res = compile_source(source, output_values=['abi', 'bin', 'metadata'], solc_binary=solc_bin,
                             optimize=optimization_enambled, optimize_runs=optimizer_runs)
    except:
        logger.exception('Compilation error')
        raise RuntimeError('Compilation failed')
    try:
        contract_res = res['<stdin>:{}'.format(contract_name)]
    except KeyError:
        contract_res = res[contract_name]
    return contract_res


def get_solc_bin_path(compiler_version):
    commit = compiler_version.split('.')[-1]
    return solc.install.get_executable_path(commit)


INSTALL_HARD_LIMIT = 60 * 15


def wait_install_solc(identifier):
    guard_path = '/tmp/solc_install_guard_{}'.format(identifier)
    start_time = time.time()

    if os.path.exists(guard_path):
        logger.debug('Solc install guard %s exists, wait installation finish', identifier)
        while os.path.exists(guard_path):
            time.sleep(5)
            if time.time() - start_time > INSTALL_HARD_LIMIT:
                logger.debug('Solc %s install wait aborted', identifier)
                return False
        logger.debug('Solc install guard %s removed, installation done', identifier)
        return True

    with open(guard_path, 'a'):
        pass
    logger.debug('Installing solc, version %s', identifier)
    try:
        install_solc(identifier)
    finally:
        os.unlink(guard_path)
    logger.debug('Solc installed, version %s, time %s', identifier, time.time() - start_time)
    return True


def cut_contract_metadata_hash(byte_code):
    """
    https://github.com/ethereum/solidity/blob/c9bdbcf470f4ca7f8d2d71f1be180274f534888d/libsolidity/interface/CompilerStack.cpp#L699

    bytes cborEncodedHash =
        // CBOR-encoding of the key "bzzr0"
        bytes{0x65, 'b', 'z', 'z', 'r', '0'}+
        // CBOR-encoding of the hash
        bytes{0x58, 0x20} + dev::swarmHash(metadata).asBytes();
    bytes cborEncodedMetadata;
    if (onlySafeExperimentalFeaturesActivated(_contract.sourceUnit().annotation().experimentalFeatures))
        cborEncodedMetadata =
            // CBOR-encoding of {"bzzr0": dev::swarmHash(metadata)}
            bytes{0xa1} +
            cborEncodedHash;
    else
        cborEncodedMetadata =
            // CBOR-encoding of {"bzzr0": dev::swarmHash(metadata), "experimental": true}
            bytes{0xa2} +
            cborEncodedHash +
            bytes{0x6c, 'e', 'x', 'p', 'e', 'r', 'i', 'm', 'e', 'n', 't', 'a', 'l', 0xf5};
    solAssert(cborEncodedMetadata.size() <= 0xffff, "Metadata too large");
    """

    cbor_hash = '65627a7a72305820(.*)'
    sig_1 = 'a1' + cbor_hash + '0029'
    sig_2 = 'a2' + cbor_hash + '6c6578706572696d656e74616cf50037'

    m = re.match('.*({}).*'.format(sig_1), byte_code)
    if m:
        logger.debug('Metadata signature found: %s', m.groups()[0])
        swarm_hash = m.groups()[1]
        return re.sub(swarm_hash, '', byte_code), swarm_hash

    m = re.match('.*({}).*'.format(sig_2), byte_code)
    if m:
        logger.debug('Metadata experimental signature found: %s', m.groups()[0])
        swarm_hash = m.groups()[1]
        return re.sub(swarm_hash, '', byte_code), swarm_hash
    logger.debug('No metadata hash found')
    return byte_code, ''
