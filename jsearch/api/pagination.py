import decimal

from typing import NamedTuple, List, Dict, Any, Optional
from yarl import URL

from jsearch.api.ordering import Ordering


class Link(NamedTuple):
    url: URL
    kwargs: Dict[str, Any]

    def __str__(self) -> str:
        absolute_url = self.url.with_query(**self.kwargs)
        return str(absolute_url)


class PaginationBlock(NamedTuple):
    link: Optional[Link]
    next_link: Optional[Link]

    items: List[Any]

    def to_dict(self) -> Dict[str, Any]:
        block: Dict[str, Any] = {
            "next": None,
            "link": None,
            "next_kwargs": None,
            "link_kwargs": None
        }

        if self.next_link:
            block.update({
                "next": str(self.next_link),
                "next_kwargs": self.next_link.kwargs,
            })

        if self.link:
            block.update({
                "link": str(self.link),
                "link_kwargs": self.link.kwargs
            })

        return block


def stringify_link_value(value, decimals_to_ints: bool) -> str:
    if decimals_to_ints and isinstance(value, decimal.Decimal):
        # If a value is a big decimal, it will be converted to a scientific
        # notation when casted to string (i.e. str(1..0.0) = '1e+18').
        value = int(value)

    return str(value)


def make_link_query(
        fields: List[str],
        item: Dict[str, Any],
        params: Dict[str, Any],
        decimals_to_ints: bool,
        mapping: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    query = {mapping and mapping.get(field) or field: item[field] for field in fields}
    query.update(params)

    return {key: stringify_link_value(value, decimals_to_ints) for key, value in query.items()}


def get_pagination_description(
        url: URL,
        limit: int,
        ordering: Ordering,
        items: List[Dict[str, Any]],
        key_set_fields: Optional[List[str]] = None,
        mapping: Optional[Dict[str, str]] = None,
        url_params: Optional[Dict[str, Any]] = None,
        decimals_to_ints: bool = False,
) -> PaginationBlock:
    """
    If there we have (limit + 1) items - we can
    create link to next chunk. Next chunk starts
    from (limit + 1) item.
    """
    params = {
        'order': ordering.direction,
        'limit': limit,
    }
    if url_params:
        params.update(url_params)

    # based on last element - cursor link to next chunk
    next_link = None
    if len(items) > limit:
        next_chunk_item = items[-1]
        next_link_kwargs = make_link_query(
            key_set_fields or ordering.fields,
            next_chunk_item,
            mapping=mapping,
            params=params,
            decimals_to_ints=decimals_to_ints,
        )
        next_link = Link(url, next_link_kwargs)
        items = items[:-1]

    # cursor on current chunk - based on first element
    link = None
    if items:
        link_kwargs = make_link_query(
            key_set_fields or ordering.fields,
            items[0],
            mapping=mapping,
            params=params,
            decimals_to_ints=decimals_to_ints,
        )
        link = Link(url, link_kwargs)

    return PaginationBlock(link=link, next_link=next_link, items=items)
