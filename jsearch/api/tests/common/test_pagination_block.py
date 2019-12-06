import pytest
from typing import Generator, Dict, Any, NamedTuple, Optional
from yarl import URL

from jsearch.api.ordering import Ordering, ORDER_SCHEME_BY_NUMBER, ORDER_ASC
from jsearch.typing import OrderDirection


def block_generator(n: int) -> Generator[Dict[str, Any], None, None]:
    for i in range(1, n + 1):
        yield {
            'number': i
        }


@pytest.fixture()
def ordering_example() -> Ordering:
    from jsearch.common.tables import blocks_t
    return Ordering(
        scheme=ORDER_SCHEME_BY_NUMBER,
        table_columns=[blocks_t.c.number],
        direction=ORDER_ASC
    )


class PaginationCase(NamedTuple):
    label: str

    items: int
    limit: int

    ordering: OrderDirection = ORDER_ASC

    next: Optional[int] = None
    link: Optional[int] = None


cases = [
    PaginationCase('full_filled', items=21, limit=20, next=21, link=1),
    PaginationCase('only_link', items=20, limit=20, next=None, link=1),
    PaginationCase('all_empty', items=0, limit=20, next=None, link=None),
]


@pytest.mark.parametrize('case', cases, ids=[case.label for case in cases])
def test_pagination(ordering_example: Ordering, case: PaginationCase) -> None:
    from jsearch.api.pagination import get_pagination_description
    # given
    url = URL("/v1/blocks")

    # when
    pagination = get_pagination_description(
        url=url,
        limit=case.limit,
        ordering=ordering_example,
        items=list(block_generator(case.items)),
    ).to_dict()

    # then
    assert 'next' in pagination
    assert 'next_kwargs' in pagination

    assert 'link' in pagination
    assert 'link_kwargs' in pagination

    if case.link:
        assert pagination['link'] == f'/v1/blocks?' \
                                     f'number={case.link}&' \
                                     f'order={case.ordering}&' \
                                     f'limit={case.limit}'
        assert URL(pagination['link']).query == pagination['link_kwargs']
    else:
        assert pagination['link'] is None
        assert pagination['link_kwargs'] is None

    if case.next:
        assert pagination['next'] == f'/v1/blocks?' \
                                     f'number={case.next}&' \
                                     f'order={case.ordering}&' \
                                     f'limit={case.limit}'
        assert URL(pagination['next']).query == pagination['next_kwargs']
    else:
        assert pagination['next'] is None
        assert pagination['next_kwargs'] is None
