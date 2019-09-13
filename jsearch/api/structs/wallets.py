import json
from collections import OrderedDict

from typing import NamedTuple, List, Dict, Any


class WalletEvent(NamedTuple):
    type: str
    event_index: int
    event_data: Dict[str, Any]

    transaction: Dict[str, Any]

    def __getitem__(self, item):
        if isinstance(item, str):
            if item in self.transaction:
                return self.transaction[item]

            return getattr(self, item, None)
        return super(WalletEvent, self).__getitem__(item)

    @property
    def tx_hash(self):
        return self.transaction['hash']

    def to_dict(self):
        data = self.event_data
        if isinstance(data, str):
            data = json.loads(data)

        data = [{'fieldName': name, 'fieldValue': value} for name, value in data.items()]

        return {
            'eventType': self.type,
            'eventIndex': self.event_index,
            'eventData': data,
        }


class WalletEventTransaction(NamedTuple):
    transaction: Dict[str, Any]
    events: List[WalletEvent]

    def __len__(self):
        return len(self.events)

    def to_dict(self):
        return {
            'transaction': self.transaction,
            'events': [event.to_dict() for event in self.events]
        }


def wallet_events_to_json(events: List[WalletEvent]) -> List[Dict[str, Any]]:
    txs = OrderedDict()

    for event in events:
        tx = txs.get(event.tx_hash)
        if tx is None:
            tx = WalletEventTransaction(transaction=event.transaction, events=[event])
            txs[event.tx_hash] = tx
        else:
            tx.events.append(event)

    return [tx.to_dict() for tx in txs.values()]
