import logging.config
import sys

import sentry_sdk

from jsearch import settings

sentry_sdk.init(settings.RAVEN_DSN)


def configure(log_level):
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'class': 'logging.Formatter',
                'format': '%(asctime)-15s %(levelname)-8s %(name)s: %(message)s'
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
    if log_level == 'DEBUG':
        config['loggers']['sqlalchemy.engine'] = {'level': 'DEBUG', 'handlers': ['console']}
    from pprint import pprint
    pprint(config)
    logging.config.dictConfig(config)
