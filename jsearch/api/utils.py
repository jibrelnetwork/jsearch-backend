import typing
from eth_utils import ValidationError
from webargs.aiohttpparser import AIOHTTPParser

from jsearch.api.helpers import api_error


class AIOHTTPParserWithOverridenErrorsHandler(AIOHTTPParser):
    DEFAULT_VALIDATION_STATUS = 400

    def handle_error(
            self,
            error: ValidationError,
            *args,
            **kwargs
    ) -> "typing.NoReturn":
        error.messages = api_error(errors=error.messages)
        super(AIOHTTPParserWithOverridenErrorsHandler, self).handle_error(error, *args, **kwargs)


parser = AIOHTTPParserWithOverridenErrorsHandler()
use_args = parser.use_args  # type: typing.Callable
use_kwargs = parser.use_kwargs  # type: typing.Callable
