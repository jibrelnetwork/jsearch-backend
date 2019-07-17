from typing import NamedTuple, Tuple, Any, Mapping, Dict


class NotableAccount(NamedTuple):
    address: str
    name: str
    labels: Tuple[str, ...]

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> 'NotableAccount':
        return NotableAccount(
            address=mapping['address'],
            name=mapping['name'],
            labels=tuple(mapping['labels']),
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            'address': self.address,
            'name': self.name,
            'labels': list(self.labels),
        }
