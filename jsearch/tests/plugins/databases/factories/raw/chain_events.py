import json
from collections import defaultdict
from dataclasses import dataclass, field

import factory
import itertools
import pytest
from factory import DictFactory
from functools import partial
from sqlalchemy.engine import Engine
from typing import Dict, Any, Tuple, List, Set, Optional, Generator

from jsearch.api.helpers import ChainEvent
from jsearch.tests.plugins.databases.factories.common import generate_address, generate_psql_timestamp
from jsearch.tests.plugins.databases.factories.raw.body import RawDBBodyFactory
from jsearch.tests.plugins.databases.factories.raw.headers import RawDBHeadersFactory
from jsearch.tests.plugins.databases.factories.raw.receipts import RawDBReceiptsFactory
from jsearch.tests.plugins.databases.factories.raw.rewards import RawDBRewardFactory
from jsearch.utils import Singleton


class RawDBChainEventFactory(DictFactory):
    block_number = factory.sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)
    parent_block_hash = factory.LazyFunction(generate_address)
    common_block_hash = factory.LazyFunction(generate_address)
    common_block_number = factory.sequence(lambda n: n)
    type = 'create'
    drop_length = 1
    drop_block_hash = factory.LazyFunction(generate_address)
    add_length = 1
    add_block_hash = factory.LazyFunction(generate_address)
    node_id = ''
    created_at = factory.LazyFunction(generate_psql_timestamp())


@dataclass
class Block:
    number: int
    hash: str

    def __repr__(self):
        return f"<Block {self.number}: {self.hash}/>"

    def as_chain(self):
        return Chain(blocks=[self])


@dataclass
class Chain:
    blocks: List[Block] = field(default_factory=list)

    @property
    def head(self) -> Block:
        return self.blocks[0]

    @property
    def last(self) -> Block:
        return self.blocks[-1]

    def append(self, other: Block) -> None:
        self.blocks.append(other)

    def add(self, chain: 'Chain') -> None:
        self.blocks.extend(chain.blocks)

    def drop(self, chain: 'Chain') -> None:
        drop_length = len(chain.blocks)
        assert set(chain.blocks) == set(self.blocks[-1 * drop_length:])

        self.blocks = self.blocks[:-1 * drop_length]

    def replace(self, to_add: 'Chain', to_drop: 'Chain') -> None:
        self.drop(to_drop)
        self.add(to_add)

    def get_slice(self, offset: int, length: int = 1) -> 'Chain':
        start_index = -1 * offset
        stop_index = start_index + length
        return Chain(self.blocks[start_index: stop_index])

    def __repr__(self) -> str:
        blocks = ", ".join(map(repr, self.blocks))
        return f"<Chain {blocks}/>"

    def __len__(self) -> int:
        return len(self.blocks)


@dataclass
class ChainPointer:
    offset: int
    capacity: int = 0

    def get_hash(self, chain: Chain) -> str:
        return chain.get_slice(self.offset + 1, 1).last.hash

    def next(self, chain: Chain) -> str:
        if self.capacity:
            self.offset -= 1
            self.capacity -= 1
        return self.get_hash(chain)


@dataclass
class RawDbCollector(Singleton):
    bodies: List[Dict[str, Any]] = field(default_factory=list)
    events: List[Dict[str, Any]] = field(default_factory=list)
    headers: List[Dict[str, Any]] = field(default_factory=list)
    receipts: List[Dict[str, Any]] = field(default_factory=list)
    rewards: List[Dict[str, Any]] = field(default_factory=list)

    fields = {
        'chain_events': [
            'block_number',
            'block_hash',
            'parent_block_hash',
            'type',
            'common_block_number',
            'common_block_hash',
            'drop_length',
            'drop_block_hash',
            'add_length',
            'add_block_hash',
            'node_id',
            'created_at'
        ],
        'headers': [
            'block_hash',
            'block_number',
            'fields'
        ],
        'receipts': [
            'block_hash',
            'block_number',
            'fields'
        ],
        'rewards': [
            'address',
            'block_hash',
            'block_number',
            'fields'
        ],
        'bodies': [
            'block_hash',
            'block_number',
            'fields'
        ]
    }

    def add_event(self, events: Dict[str, Any]) -> None:
        self.events.append(events)

    def add_block(
            self,
            body: Dict[str, Any],
            headers: Dict[str, Any],
            receipt: Dict[str, Any],
            reward: Dict[str, Any]
    ) -> None:
        self.bodies.append(body)
        self.headers.append(headers)
        self.receipts.append(receipt)
        self.rewards.append(reward)

    def flush(self, db: Engine) -> None:
        self._insert_values(db, 'chain_events', self.events)
        self._insert_values(db, 'headers', self.headers)
        self._insert_values(db, 'rewards', self.rewards)
        self._insert_values(db, 'receipts', self.receipts)
        self._insert_values(db, 'bodies', self.bodies)

        RawDBChainEventFactory.reset_sequence(force=True)

        self.events.clear()
        self.headers.clear()
        self.rewards.clear()
        self.receipts.clear()
        self.bodies.clear()

    def _insert_values(self, db: Engine, table: str, values: List[Dict[str, Any]]) -> None:
        fields = self.fields[table]
        values = [_get_values(fields, item) for item in values]
        query = _get_insert(table, fields)
        if values:
            db.execute(query, values)

    def draw(self):
        draw_events(*self.events)


def _get_values(fields, source: Dict[str, Any]) -> Generator[str, None, None]:
    for key in fields:
        value = source[key]
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        yield value


def _get_insert(table: str, fields: List[str]) -> str:
    return f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({', '.join(('%s',) * len(fields))});"


@dataclass
class ChainGenerator:
    node_id: str
    chain: Chain = field(default_factory=Chain)
    collector: RawDbCollector = field(default_factory=RawDbCollector)
    factory: DictFactory = RawDBChainEventFactory

    def create_chain(
            self,
            n: int,
            start_from: Optional[int] = None,
            start_from_hash: Optional[str] = None
    ) -> 'ChainGenerator':
        parent_hash = start_from_hash
        for i in range(0, n):
            self.create_block(start_from + i, parent_hash)
        return self.get_slice(n, n)

    def create_block(self, block_number: Optional[int] = None, parent_block_hash: Optional[str] = None) -> None:
        block_number = block_number or self.chain and self.chain.last.number + 1 or 0
        parent_block_hash = parent_block_hash or self.chain and self.chain.last.hash or ''

        event = self.factory.create(
            type=ChainEvent.INSERT,
            node_id=self.node_id,
            parent_block_hash=parent_block_hash,
            block_number=block_number,
            common_block_number=block_number,
        )

        block = Block(number=event['block_number'], hash=event['block_hash'])
        body = RawDBBodyFactory.create(block_number=block.number, block_hash=block.hash)
        headers = RawDBHeadersFactory.create(block_number=block.number, block_hash=block.hash)
        receipts = RawDBReceiptsFactory.create(block_number=block.number, block_hash=block.hash)
        rewards = RawDBRewardFactory.create(block_number=block.number, block_hash=block.hash)

        self.chain.append(block)
        self.collector.add_event(event)
        self.collector.add_block(body, headers, receipts, rewards)

    def add_chain(self, chain: Chain) -> None:
        event = self.factory.create(
            type=ChainEvent.SPLIT,
            node_id=self.node_id,
            common_block_number=self.chain.head,
            common_block_hash=self.chain.head,
            add_length=len(chain),
            add_block_hash=chain.head.hash
        )
        self.collector.add_event(event)
        self.chain.add(chain)

    def drop_chain(self, chain: Chain) -> None:
        event = self.factory.create(
            type=ChainEvent.SPLIT,
            node_id=self.node_id,
            common_block_number=self.chain.head,
            common_block_hash=self.chain.head,
            drop_length=len(chain),
            drop_block_hash=chain.head.hash
        )
        self.collector.add_event(event)
        self.chain.add(chain)

    def replace(self, to_add: Chain, to_drop: Chain) -> None:
        event = self.factory.create(
            type=ChainEvent.SPLIT,
            node_id=self.node_id,
            common_block_number=self.chain.head,
            common_block_hash=self.chain.head,
            drop_length=len(to_drop),
            drop_block_hash=to_drop.head.hash,
            add_length=len(to_add),
            add_block_hash=to_add.head.hash
        )
        self.collector.add_event(event)
        self.chain.replace(to_add, to_drop)

    def get_slice(self, offset: int, length: int = 1) -> 'ChainGenerator':
        chain_slice = self.chain.get_slice(offset, length)
        return ChainGenerator(chain=chain_slice, collector=self.collector, node_id=self.node_id)


BLOCK = '[x]'
SPLIT = '[>]'
BLOCK_SPACING = ' | '
SPLIT_SPACING = ' / '
NODE_OFFSET = ' . '
SPACE = ' '

get_spacing = {
    BLOCK: BLOCK_SPACING,
    SPLIT: SPLIT_SPACING,
    NODE_OFFSET: SPACE
}.get


def get_chains(
        graph: Dict[str, List[str]],
        cache: Optional[Set[Tuple[str, str]]] = None,
        path: Optional[List[Tuple[str, str]]] = None,
        vertex: Optional[str] = None,
) -> List[List[Tuple[str, str]]]:
    cache = cache or set()
    path = path or list()
    if vertex is not None:
        next_vertexes = graph.get(vertex) or []
    else:
        next_vertexes = graph.keys()
    next_vertexes = sorted(next_vertexes)

    paths = []
    for next_vertex in next_vertexes:
        edges = {(vertex, next_vertex), (next_vertex, vertex)}
        if not edges & cache:
            cache.add((vertex, next_vertex))
            new_paths = get_chains(graph, cache, [*path, next_vertex], next_vertex)
            if new_paths:
                paths.extend(new_paths)
            else:
                paths.append([*path, next_vertex])

    return [path for path in paths if len(path) > 1]


def draw_events(*events: Tuple[Dict[str, Any]]) -> None:
    """
    >>> draw_events( \
        {'node_id': 'l', 'block_number': 2, 'block_hash': '0x02', 'parent_block_hash': '0x01'}, \
        {'node_id': 'l', 'block_number': 3, 'block_hash': '0x03', 'parent_block_hash': '0x02'}, \
        {'node_id': 'l', 'block_number': 4, 'block_hash': '0x04', 'parent_block_hash': '0x03'}, \
        {'node_id': 'l', 'block_number': 3, 'block_hash': '0x03i', 'parent_block_hash': '0x02'}, \
        {'node_id': 'r', 'block_number': 3, 'block_hash': '0x03i', 'parent_block_hash': '0x01'}, \
        {'node_id': 'r', 'block_number': 4, 'block_hash': '0x04', 'parent_block_hash': '0x03i'}, \
        {'node_id': 'r', 'block_number': 4, 'block_hash': '0x04i', 'parent_block_hash': '0x03i'}, \
        {'node_id': 'r', 'block_number': 5, 'block_hash': '0x05i', 'parent_block_hash': '0x04i'}, \
        {'node_id': 'r', 'block_number': 5, 'block_hash': '0x05s', 'parent_block_hash': '0x04i'}, \
        {'node_id': 'r', 'block_number': 6, 'block_hash': '0x06i', 'parent_block_hash': '0x05i'}, \
    )
    -[0x01]-[0x02]-[0x03]-[0x04]
    -[0x01]-[0x02]-[0x03i]
    -[0x01]-[0x03i]-[0x04]
    -[0x01]-[0x03i]-[0x04i]-[0x05i]-[0x06i]
    -[0x01]-[0x03i]-[0x04i]-[0x05s]
    """
    vertexes = {}
    edges = defaultdict(partial(defaultdict, list))

    def get_item(key, x):
        return x[key]

    get_node = partial(get_item, 'node_id')
    get_hash = partial(get_item, 'block_hash')
    get_number = partial(get_item, 'block_number')
    get_parent = partial(get_item, 'parent_block_hash')

    graphs = []
    for node, group in itertools.groupby(events, key=get_node):
        vertexes.update({get_hash: get_number(event) for event in events})

        for event in group:
            left_vertex = get_hash(event)
            right_vertex = get_parent(event)

            edges[node][left_vertex].append(right_vertex)
            edges[node][right_vertex].append(left_vertex)

        paths = sorted(get_chains(graph=edges[node]))
        graphs.append(paths)

    for graph in graphs:
        for i, line in enumerate(graph):
            line_str = ''
            for j, value in enumerate(line):
                line_str += f'-[{value}]'
            print(line_str)  # NOQA


@pytest.fixture
def chain_generator() -> ChainGenerator:
    yield ChainGenerator
    RawDBChainEventFactory.reset_sequence(force=True)
