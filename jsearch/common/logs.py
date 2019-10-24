import logging.config
import sys

import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from jsearch import settings

sentry_sdk.init(
    settings.SENTRY_DSN,
    integrations=[AioHttpIntegration()],
)


def select_formatter_class(no_json_formatter: bool) -> str:
    if no_json_formatter:
        return 'logging.Formatter'

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
