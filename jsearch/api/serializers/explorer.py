from typing import Dict, Any

from marshmallow import Schema, ValidationError
from marshmallow.fields import Str
from marshmallow.validate import Length, OneOf

from jsearch.api.ordering import ORDER_DESC, ORDER_ASC
from jsearch.api.serializers.common import convert_to_api_error_and_raise
from jsearch.api.serializers.fields import StrLower


class InternalTransactionsSchema(Schema):
    txhash = StrLower(validate=Length(min=1, max=100), location='match_info')
    order = Str(
        missing=ORDER_DESC,
        validate=OneOf([ORDER_ASC, ORDER_DESC], error='Ordering can be either "asc" or "desc".'),
    )

    def handle_error(self, exc: ValidationError, data: Dict[str, Any]) -> None:
        convert_to_api_error_and_raise(exc)
