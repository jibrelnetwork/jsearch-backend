import logging.config
import sys


def configure(loglevel):
    logging.config.dictConfig({
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
                'level': loglevel,
                'handlers': ['console'],
            },
            'syncer': {
                'level': loglevel,
                'handlers': ['console'],
            },
            '': {
                'level': loglevel,
                'handlers': ['console']
            }
        }
    })
