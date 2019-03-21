import json
from typing import Dict, List, Any, Optional, Union

from jsearch.api.models import Block, Transaction, Account, TokenTransfer, Receipt


def get_default_type(model, key):
    if issubclass(model.swagger_types[key], list):
        return list()
    return None


class ModelFromDumpWrapper:
    __table__: str

    def __init__(self, **kwargs):
        self.entity = self._create_entity(**kwargs)

    def __str__(self):
        return json.dumps(self.as_dict())

    def _create_entity(self, **data):
        data = {key: value for key, value in data.items() if key in self.model.attribute_map}
        data.update(
            {key: get_default_type(self.model, key) for key in set(self.model.attribute_map) - set(data.keys())}
        )
        return self.model(**data)

    @property
    def model(self):
        raise NotImplementedError

    @classmethod
    def from_dump(cls,
                  dump: Dict[str, List[Dict[str, Any]]],
                  filters: Optional[Dict[str, any]] = None,
                  strict: bool = True,
                  index: Optional[int] = None,
                  bulk: bool = False,
                  **kwargs) -> Union[List[object], object]:
        """
        Get objects from dump

        Args:
            dump: dump loaded from json fixture
            filters: filters to reduce objects
            strict: if True when need all filters to get element
            index: if not None when will get element by index
            bulk: if True when will return list of objects
        """
        data = dump[cls.__table__]
        filters = filters or {}

        items = []
        for record in data:
            conditions = (record[field] == value for field, value in filters.items())

            operator = all if strict else any
            if not filters or operator(conditions):
                items.append(record)

        if bulk:
            return [cls(**item, **kwargs) for item in items]

        if not bulk and index is not None:
            return cls(**items[index], **kwargs)

        if not bulk and len(items) > 1:
            raise ValueError('Too many items')

        if len(items) < 1:
            raise ValueError('Item was not found')

        return cls(**items[0], **kwargs)

    def as_dict(self):
        return self.entity.to_dict()


class BlockFromDumpWrapper(ModelFromDumpWrapper):
    model = Block
    __table__ = "blocks"


class TransactionFromDumpWrapper(ModelFromDumpWrapper):
    model = Transaction
    __table__ = "transactions"


class AccountBaseFromDumpWrapper(ModelFromDumpWrapper):
    model = Account
    __table__ = 'accounts_base'


class AccountStateFromDumpWrapper(ModelFromDumpWrapper):
    model = Account
    __table__ = 'accounts_state'


class TokenTransferFromDumpWrapper(ModelFromDumpWrapper):
    model = TokenTransfer
    __table__ = 'logs'

    def _create_entity(self, **data):
        return self.model.from_log_record(data)


class ReceiptFromDumpWrapper(ModelFromDumpWrapper):
    model = Receipt
    __table__ = 'receipts'
