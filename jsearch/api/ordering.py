from operator import le, ge, gt, lt

from sqlalchemy import asc, desc
from sqlalchemy.sql import CompoundSelect
from typing import Optional, NamedTuple

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
    scheme: OrderScheme
    table_columns: Columns
    direction: OrderDirection

    @property
    def apply_direction(self):
        return DIRECTIONS[self.direction]

    @property
    def operator_or_equal(self):
        return DIRECTIONS_OPERATOR_OR_EQUAL_MAPS[self.direction]

    @property
    def operator(self):
        return DIRECTIONS_OPERATOR_MAPS[self.direction]

    @property
    def columns(self):
        return [self.apply_direction(column) for column in self.table_columns]

    @property
    def fields(self):
        return [column.name for column in self.table_columns]

    def get_ordering_for_union_query(self, table: CompoundSelect):
        return [self.apply_direction(getattr(table.c, field)) for field in self.fields]

    def reverse(self):
        direction = ORDER_ASC if self.direction == ORDER_DESC else ORDER_DESC
        return get_ordering(
            columns=self.table_columns,
            scheme=self.scheme,
            direction=direction
        )


def get_order_schema(timestamp: Optional[int]) -> OrderScheme:
    if timestamp is None:
        return ORDER_SCHEME_BY_NUMBER

    return ORDER_SCHEME_BY_TIMESTAMP


def get_ordering(columns: Columns, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
    return Ordering(
        scheme=scheme,
        direction=direction,
        table_columns=columns
    )
