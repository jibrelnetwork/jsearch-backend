import logging.config
import sys


def configure(loglevel):
    logging.config.dictConfig({
        'version': 1,
        'formatters': {
            'default': {
                'class': 'logging.Formatter',
                'format': '%(asctime)s %(levelname)-8s %(message)s'
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
            'tools': {
                'level': loglevel,
                'handlers': ['console'],
            }
        }
    })
