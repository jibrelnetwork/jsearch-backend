from marshmallow import Schema
from marshmallow.fields import Str
from marshmallow.validate import Length, OneOf

from jsearch.api.ordering import ORDER_DESC, ORDER_ASC
from jsearch.api.serializers.fields import StrLower


class InternalTransactionsSchema(Schema):
    txhash = StrLower(validate=Length(min=1, max=100), location='match_info')
    order = Str(
        missing=ORDER_DESC,
        validate=OneOf([ORDER_ASC, ORDER_DESC], error='Ordering can be either "asc" or "desc".'),
    )
