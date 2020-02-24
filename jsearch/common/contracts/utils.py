import binascii

from ethereum.abi import ContractTranslator
from ethereum.utils import decode_hex


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
