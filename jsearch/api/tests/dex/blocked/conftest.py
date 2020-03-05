import itertools
from functools import partial
from random import randint
from typing import Callable, Dict, Any, Optional, List

import pytest
import yarl
from aiohttp import web

from jsearch.common.processing.dex_logs import DexEventType
from jsearch.tests.plugins.databases.factories.common import generate_address


@pytest.fixture()
def get_url(app: web.Application) -> Callable[[str], yarl.URL]:
    def get_url(user_address: str) -> yarl.URL:
        return app.router['dex_blocked'].url_for(user_address=user_address)

    return get_url


@pytest.fixture()
def url(get_url: Callable[[str], yarl.URL], user_address: str) -> yarl.URL:
    return get_url(user_address)


@pytest.fixture()
def user_address():
    return generate_address()


@pytest.fixture()
def random_assets_locks_and_unlocks(
        user_address: str,
        dex_log_factory: Callable[..., Dict[str, Any]]
) -> Callable[[int], List[Dict[str, Any]]]:
    def add_event(history, **kwargs: Any) -> Dict[str, Any]:
        event = dex_log_factory(**kwargs)
        payload = event.pop('event_data')
        last = {
            **event,
            **payload,
        }
        history.append(last)
        return last

    def _factory(
            limit: int = 10,
            user: Optional[str] = None,
            tokens: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        history = []
        user = user or user_address
        tokens = tokens or [generate_address(), generate_address(), generate_address()]

        while len(history) < limit:
            seed = randint(0, 1)

            add_blocked = partial(add_event, history, event_type=DexEventType.TOKEN_BLOCKED)
            add_unblocked = partial(add_event, history, event_type=DexEventType.TOKEN_UNBLOCKED)

            action = [
                add_blocked,
                add_unblocked
            ][seed]

            token = tokens[randint(0, len(tokens) - 1)]
            kwargs = {
                'userAddress': user
            }
            if token:
                kwargs['assetAddress'] = token

            action(event_args=kwargs)

        return history

    return _factory


@pytest.fixture()
def get_total_locks():
    def aggregate(asset_events: List[Dict[str, Any]]) -> Dict[str, str]:
        total_locks = {}
        assets = sorted(asset_events, key=lambda x: x['assetAddress'])
        for asset, asset_events in itertools.groupby(assets, key=lambda x: x['assetAddress']):
            events = list(asset_events)

            locked = sum([x['assetAmount'] for x in events if x['event_type'] == DexEventType.TOKEN_BLOCKED], 0)
            unlocked = sum([x['assetAmount'] for x in events if x['event_type'] == DexEventType.TOKEN_UNBLOCKED], 0)

            total_locks[asset] = str(locked - unlocked)
        return total_locks

    return aggregate
