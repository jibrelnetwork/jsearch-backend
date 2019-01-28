from typing import Dict, Any, Tuple
from uuid import uuid4


def get_uuid(msg: Dict[str, Any]) -> str:
    if 'uuid' not in msg:
        raise ValueError('[SERVICE BUS] There is not uuid in msg: %s', msg)
    return msg['uuid']


def get_value(msg: Dict[str, Any]) -> Dict[str, Any]:
    if 'value' not in msg:
        raise ValueError('[SERVICE BUS] TThere is not value in msg: %s', msg)
    return msg['value']


def get_reply_topic(msg) -> str:
    if 'reply_to' not in msg:
        raise ValueError('[SERVICE BUS] TThere is not topic in msg: %s', msg)
    return msg['reply_to']


def make_request(value: Dict[str, Any], reply_to: str):
    return {
        'uuid': str(uuid4()),
        'value': value,
        'reply_to': reply_to
    }


def read_request(msg: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
    return get_uuid(msg), get_reply_topic(msg), get_value(msg)


def make_reply(uuid: str, value: Dict[str, Any]):
    return {
        'uuid': uuid,
        'value': value,
    }


def read_reply(msg: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    return get_uuid(msg), get_value(msg)
