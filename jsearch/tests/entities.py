import json


class ModelFromDumpWrapper:
    __table__: str

    def __init__(self, **kwargs):
        data = {key: value for key, value in kwargs.items() if key in self.model.attribute_map}
        data.update({key: None for key in set(self.model.attribute_map) - set(kwargs.keys())})
        self.entity = self.model(**data)

    def __str__(self):
        return json.dumps(self.as_dict())

    @property
    def model(self):
        raise NotImplementedError

    @classmethod
    def from_dump(cls, dump, filters, bulk=False, **kwargs):
        data = dump[cls.__table__]

        items = []
        for record in data:
            conditions = (record[field] == value for field, value in filters.items())
            if all(conditions):
                items.append(record)

        if bulk:
            return [cls(**item, **kwargs) for item in items]

        if not bulk and len(items) > 1:
            raise ValueError('Too many items')

        if len(items) < 1:
            raise ValueError('Item was not found')

        return cls(**items[0], **kwargs)

    def as_dict(self):
        return self.entity.to_dict()


class BlockFromDumpWrapper(ModelFromDumpWrapper):
    __table__ = "blocks"

    @property
    def model(self):
        from jsearch.api.models.all import Block
        return Block


class TransactionFromDumpWrapper(ModelFromDumpWrapper):
    __table__ = "transactions"

    @property
    def model(self):
        from jsearch.api.models.all import Transaction
        return Transaction
