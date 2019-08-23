import decimal

from typing import NamedTuple, List, Dict, Any, Optional
from yarl import URL

from jsearch.api.ordering import Ordering


class Page(NamedTuple):
    link: str
    next_link: str

    items: List[Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "paging": {
                "next": self.next_link,
                "link": self.link
            }
        }


def get_link(
        url: URL,
        fields: List[str],
        item: Dict[str, Any],
        params: Dict[str, Any],
        decimals_to_ints: bool,
        mapping: Optional[Dict[str, str]] = None,
) -> str:
    query = dict()

    for field in fields:
        query_key = mapping and mapping.get(field) or field
        value = item[field]
        if decimals_to_ints and isinstance(value, decimal.Decimal):
            # If a value is a big decimal, it will be converted to a scientific
            # notation when casted to string (i.e. str(1..0.0) = '1e+18').
            value = int(value)

        query[query_key] = str(value)

    absolute_url = url.with_query({**query, **params})
    if absolute_url:
        return str(absolute_url)


def get_page(
        url: URL,
        limit: int,
        ordering: Ordering,
        items: List[Dict[str, Any]],
        key_set_fields: Optional[List[str]] = None,
        mapping: Optional[Dict[str, str]] = None,
        url_params: Optional[Dict[str, Any]] = None,
        decimals_to_ints: bool = False,
) -> Page:
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

    if len(items) > limit:
        next_chunk_item = items[-1]
        next_link = get_link(
            url,
            key_set_fields or ordering.fields,
            next_chunk_item,
            mapping=mapping,
            params=params,
            decimals_to_ints=decimals_to_ints,
        )

        items = items[:-1]
    else:
        next_link = None

    if items:
        link = get_link(
            url,
            key_set_fields or ordering.fields,
            items[0],
            mapping=mapping,
            params=params,
            decimals_to_ints=decimals_to_ints,
        )
    else:
        link = None

    return Page(link=link, next_link=next_link, items=items)
