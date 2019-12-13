import asyncio

import yaml
from openapi_core import create_spec
from openapi_core.schema.schemas._format import oas30_format_checker
from openapi_core.validation.response.validators import ResponseValidator
from typing import Dict, Any, Callable, Awaitable

from aiohttp import web
from openapi_core.wrappers.base import BaseOpenAPIRequest, BaseOpenAPIResponse
from yarl import URL

from jsearch import settings
from jsearch.api.helpers import get_request_canonical_path


class AiohttpOpenAPIRequest(BaseOpenAPIRequest):
    def __init__(self, request: web.Request):
        self.request = request

    @property
    def host_url(self) -> str:
        return URL.build(host=self.request.host, scheme=self.request.scheme).human_repr()

    @property
    def path(self) -> str:
        return self.request.path

    @property
    def method(self) -> str:
        return self.request.method.lower()

    @property
    def path_pattern(self) -> str:
        return get_request_canonical_path(self.request)

    @property
    def parameters(self) -> Dict[str, Any]:
        return {
            'path': self.request._match_info or {},
            'query': self.request.query,
            'header': self.request.headers,
            'cookie': self.request.cookies,
        }

    @property
    def body(self) -> Dict[str, Any]:
        params_fut = asyncio.create_task(self.request.post())
        params = params_fut.result()

        return params

    @property
    def mimetype(self) -> str:
        return self.request.content_type


class AiohttpOpenAPIResponse(BaseOpenAPIResponse):
    def __init__(self, response: web.Response):
        self.response = response

    @property
    def data(self) -> bytes:
        return self.response.body

    @property
    def status_code(self) -> int:
        return self.response.status

    @property
    def mimetype(self) -> str:
        return self.response.content_type


spec = create_spec(yaml.safe_load(settings.SWAGGER_SPEC_FILE.read_text()))
spec_response_validator = ResponseValidator(
    spec,
    custom_formatters={
        'dec': str,
        'hex': str,
        'hash': str,
        'address': str,
        'addresses': str,
        'url': str,
    }
)


# TODO (nickgashkov): Implement format checkers.
oas30_format_checker.checks('dec')(lambda instance: True)
oas30_format_checker.checks('hex')(lambda instance: True)
oas30_format_checker.checks('hash')(lambda instance: True)
oas30_format_checker.checks('address')(lambda instance: True)
oas30_format_checker.checks('addresses')(lambda instance: True)
oas30_format_checker.checks('url')(lambda instance: True)


def validate_response(request: web.Request, response: web.Response) -> None:
    openapi_request = AiohttpOpenAPIRequest(request)
    openapi_response = AiohttpOpenAPIResponse(response)

    spec_response_validator.validate(openapi_request, openapi_response).raise_for_errors()


Handler = Callable[[web.Request], Awaitable[web.Response]]


@web.middleware
async def spec_testing_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
    response = await handler(request)

    if request.app['validate_spec']:
        validate_response(request, response)

    return response
