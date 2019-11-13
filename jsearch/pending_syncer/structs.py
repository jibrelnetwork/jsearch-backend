from typing import Optional, Dict, Any, NamedTuple


class PendingTransaction(NamedTuple):
    last_synced_id: int
    hash: str
    timestamp: str
    removed: bool
    node_id: Optional[str]
    status: Optional[str]
    r: Optional[str]
    s: Optional[str]
    v: Optional[str]
    to: Optional[str]
    from_: Optional[str]
    gas: Optional[str]
    gas_price: Optional[str]
    input: Optional[str]
    nonce: Optional[str]
    value: Optional[str]

    @classmethod
    def from_raw_tx(cls, raw_tx: Dict[str, Any]) -> 'PendingTransaction':
        return PendingTransaction(
            last_synced_id=raw_tx['id'],
            hash=raw_tx['tx_hash'],
            timestamp=raw_tx['timestamp'],
            removed=raw_tx['removed'],
            node_id=raw_tx['node_id'],
            status=raw_tx['status'],
            r=raw_tx['fields'].get('r'),
            s=raw_tx['fields'].get('s'),
            v=raw_tx['fields'].get('v'),
            to=raw_tx['fields'].get('to'),
            from_=raw_tx['fields'].get('from'),
            gas=raw_tx['fields'].get('gas'),
            gas_price=raw_tx['fields'].get('gasPrice'),
            input=raw_tx['fields'].get('input'),
            nonce=raw_tx['fields'].get('nonce'),
            value=raw_tx['fields'].get('value'),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'last_synced_id': self.last_synced_id,
            'hash': self.hash,
            'timestamp': self.timestamp,
            'removed': self.removed,
            'node_id': self.node_id,
            'status': self.status,
            'r': self.r,
            's': self.s,
            'v': self.v,
            'to': self.to,
            'from': self.from_,
            'gas': self.gas,
            'gas_price': self.gas_price,
            'input': self.input,
            'nonce': self.nonce,
            'value': self.value,
        }
