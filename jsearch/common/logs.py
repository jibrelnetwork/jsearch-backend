import logging.config
import sys

import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

from jsearch import settings

sentry_sdk.init(
    settings.RAVEN_DSN,
    integrations=[AioHttpIntegration(), SqlalchemyIntegration()],
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
                'format': '%(asctime)-15s %(levelname)-8s %(name)s: %(message)s',
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
            'kafka.conn': {
                'level': 'CRITICAL',
                'handlers': ['console']
            },
            'aiokafka': {
                'level': 'CRITICAL',
                'handlers': ['console']
            },
            'aiokafka.consumer.fetcher': {
                'level': 'CRITICAL',
                'handlers': ['console']
            },
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
