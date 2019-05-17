import logging.config
import sys

import sentry_sdk

from jsearch import settings

sentry_sdk.init(settings.RAVEN_DSN)


def configure(log_level: str) -> None:
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'class': 'pythonjsonlogger.jsonlogger.JsonFormatter',
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
