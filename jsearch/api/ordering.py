from operator import le, ge

from sqlalchemy import asc, desc
from typing import Optional, Dict, NamedTuple, List, Callable, Any

from jsearch.typing import OrderScheme, Columns, OrderDirection

ORDER_SCHEME_BY_NUMBER: OrderScheme = 'order_by_number'
ORDER_SCHEME_BY_TIMESTAMP: OrderScheme = 'order_by_timestamp'

ORDER_ASC: OrderDirection = 'asc'
ORDER_DESC: OrderDirection = 'desc'

DEFAULT_ORDER = ORDER_DESC
DIRECTIONS = {
    ORDER_ASC: asc,
    ORDER_DESC: desc
}

DIRECTIONS_OPERATOR_MAPS = {
    ORDER_ASC: ge,
    ORDER_DESC: le
}


class Ordering(NamedTuple):
    columns: Columns
    fields: List[str]
    scheme: OrderScheme
    operator: Callable[[Any, Any], Any]
    direction: OrderDirection


def get_order_schema(timestamp: Optional[int]) -> OrderScheme:
    if timestamp is None:
        return ORDER_SCHEME_BY_NUMBER

    return ORDER_SCHEME_BY_TIMESTAMP


def get_ordering(mapping: Dict[str, Columns], scheme: OrderScheme, direction: OrderDirection) -> Ordering:
    columns: Columns = mapping[scheme]
    direction_func = DIRECTIONS[direction]
    operator = DIRECTIONS_OPERATOR_MAPS[direction]

    return Ordering(
        columns=[direction_func(column) for column in columns],
        fields=[column.name for column in columns],
        scheme=scheme,
        direction=direction,
        operator=operator
    )
