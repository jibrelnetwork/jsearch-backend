from operator import le, ge, gt, lt

from sqlalchemy import asc, desc
from sqlalchemy.sql import CompoundSelect
from typing import Optional, NamedTuple, List, Callable, Any

from jsearch.typing import OrderScheme, Columns, OrderDirection

ORDER_SCHEME_NONE: OrderScheme = ''
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
    ORDER_ASC: gt,
    ORDER_DESC: lt
}

DIRECTIONS_OPERATOR_OR_EQUAL_MAPS = {
    ORDER_ASC: ge,
    ORDER_DESC: le
}


class Ordering(NamedTuple):
    columns: Columns
    fields: List[str]
    scheme: OrderScheme
    operator_or_equal: Callable[[Any, Any], Any]
    operator: Callable[[Any, Any], Any]
    direction: OrderDirection

    @property
    def apply_direction(self):
        return DIRECTIONS[self.direction]

    def get_ordering_for_union_query(self, table: CompoundSelect):
        return [self.apply_direction(getattr(table.c, field)) for field in self.fields]


def get_order_schema(timestamp: Optional[int]) -> OrderScheme:
    if timestamp is None:
        return ORDER_SCHEME_BY_NUMBER

    return ORDER_SCHEME_BY_TIMESTAMP


def get_ordering(columns: Columns, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
    apply_order_direction = DIRECTIONS[direction]
    operator = DIRECTIONS_OPERATOR_MAPS[direction]
    operator_or_equal = DIRECTIONS_OPERATOR_OR_EQUAL_MAPS[direction]

    return Ordering(
        columns=[apply_order_direction(column) for column in columns],
        fields=[column.name for column in columns],
        scheme=scheme,
        direction=direction,
        operator=operator,
        operator_or_equal=operator_or_equal
    )
