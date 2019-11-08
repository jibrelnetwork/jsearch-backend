from webargs.aiohttpparser import AIOHTTPParser


class AIOHTTPParserWithOverridenErrorsHandler(AIOHTTPParser):
    DEFAULT_VALIDATION_STATUS = 400


parser = AIOHTTPParserWithOverridenErrorsHandler()
use_args = parser.use_args
use_kwargs = parser.use_kwargs
