import weakref

from sentry_sdk._compat import reraise
from sentry_sdk.hub import Hub
from sentry_sdk.integrations import Integration
from sentry_sdk.integrations.aiohttp import _make_request_processor, _capture_exception
from sentry_sdk.integrations.logging import ignore_logger
from sentry_sdk.tracing import Span
from sentry_sdk.utils import (
    transaction_from_function,
    HAS_REAL_CONTEXTVARS,
)

import asyncio
from aiohttp.web import Application, HTTPException, UrlDispatcher

from sentry_sdk._types import MYPY

if MYPY:
    from aiohttp.web_request import Request
    from aiohttp.abc import AbstractMatchInfo
    from typing import Any


# TODO: Replace with new PyPI release after getsentry/sentry-python#571 is
# merged.


class AioHttpIntegration(Integration):
    identifier = "aiohttp"

    @staticmethod
    def setup_once():
        # type: () -> None
        if not HAS_REAL_CONTEXTVARS:
            # We better have contextvars or we're going to leak state between
            # requests.
            raise RuntimeError(
                "The aiohttp integration for Sentry requires Python 3.7+ "
                " or aiocontextvars package"
            )

        ignore_logger("aiohttp.server")

        old_handle = Application._handle

        async def sentry_app_handle(self, request, *args, **kwargs):
            # type: (Any, Request, *Any, **Any) -> Any
            async def inner():
                # type: () -> Any
                hub = Hub.current
                if hub.get_integration(AioHttpIntegration) is None:
                    return await old_handle(self, request, *args, **kwargs)

                weak_request = weakref.ref(request)

                with Hub(Hub.current) as hub:
                    with hub.configure_scope() as scope:
                        scope.clear_breadcrumbs()
                        scope.add_event_processor(_make_request_processor(weak_request))

                    span = Span.continue_from_headers(request.headers)
                    span.op = "http.server"
                    # If this transaction name makes it to the UI, AIOHTTP's
                    # URL resolver did not find a route or died trying.
                    span.transaction = "generic AIOHTTP request"

                    with hub.start_span(span):
                        try:
                            response = await old_handle(self, request)
                        except HTTPException:
                            raise
                        # === Start diverge from the library ===
                        except asyncio.CancelledError:
                            raise
                        # === End diverge from the library ===
                        except Exception:
                            reraise(*_capture_exception(hub))

                        return response

            # Explicitly wrap in task such that current contextvar context is
            # copied. Just doing `return await inner()` will leak scope data
            # between requests.
            return await asyncio.get_event_loop().create_task(inner())

        Application._handle = sentry_app_handle

        old_urldispatcher_resolve = UrlDispatcher.resolve

        async def sentry_urldispatcher_resolve(self, request):
            # type: (UrlDispatcher, Request) -> AbstractMatchInfo
            rv = await old_urldispatcher_resolve(self, request)

            name = None

            try:
                name = transaction_from_function(rv.handler)
            except Exception:
                pass

            if name is not None:
                with Hub.current.configure_scope() as scope:
                    scope.transaction = name

            return rv

        UrlDispatcher.resolve = sentry_urldispatcher_resolve
