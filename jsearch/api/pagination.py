from typing import NamedTuple, List, Dict, Any

from jsearch.api.structs import Ordering


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


def get_link(url, fields: List[str], item: Dict[str, Any], mapping: Dict[str, str], params: Dict[str, Any]) -> str:
    query = {mapping.get(key) or key: item[key] for key in fields}
    absolute_url = url.with_query({**query, **params})
    if absolute_url:
        return str(absolute_url)


def get_page(
        url,
        ordering: Ordering,
        items: List[Dict[str, Any]],
        mapping: Dict[str, str],
        limit: int,
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

    if len(items) > limit:
        next_chunk_item = items[-1]
        next_link = get_link(url, ordering.fields, next_chunk_item, mapping=mapping, params=params)

        items = items[:-1]
    else:
        next_link = None

    if items:
        link = get_link(url, ordering.fields, items[0], mapping=mapping, params=params)
    else:
        link = None

    return Page(link=link, next_link=next_link, items=items)
