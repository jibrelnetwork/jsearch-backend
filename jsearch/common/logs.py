import logging.config
import sys
from collections import OrderedDict

import sentry_sdk
from pythonjsonlogger import jsonlogger
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from jsearch import settings

sentry_sdk.init(
    settings.SENTRY_DSN,
    integrations=[AioHttpIntegration()],
)


class FlatJsonFormatter(jsonlogger.JsonFormatter):

    def format(self, record):
        if record.exc_info:
            return super(jsonlogger.JsonFormatter, self).format(record)

        message_dict = {}
        if isinstance(record.msg, dict):
            message_dict = record.msg
            record.message = None
        else:
            record.message = record.getMessage()

        # only format time if needed
        if "asctime" in self._required_fields:
            record.asctime = self.formatTime(record, self.datefmt)

        log_record = OrderedDict()

        self.add_fields(log_record, record, message_dict)
        log_record = self.process_log_record(log_record)

        log_record = " ".join([f"{key}: {value}" for key, value in log_record.items()])
        return "%s%s" % (self.prefix, log_record)


def select_formatter_class(no_json_formatter: bool) -> str:
    if no_json_formatter:
        return 'jsearch.common.logs.FlatJsonFormatter'

    return 'pythonjsonlogger.jsonlogger.JsonFormatter'


def configure(log_level: str, formatter_class: str) -> None:
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'class': formatter_class,
                'format': '%(process) %(asctime)-15s %(levelname)-8s %(name)s: %(message)s',
            }
        },
        'handlers': {
            'console': {
                '()': 'logging.StreamHandler',
                'stream': sys.stdout,
                'formatter': 'default'
            },
        },
        'loggers': {
            'post_processing': {
                'level': log_level,
                'handlers': ['console'],
            },
            'syncer': {
                'level': log_level,
                'handlers': ['console'],
            },
            '': {
                'level': log_level,
                'handlers': ['console']
            }
        }
    }
    logging.config.dictConfig(config)
