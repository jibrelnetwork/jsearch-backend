import os

from jsearch.settings import JSEARCH_MAIN_DB, LOG_LEVEL, NO_JSON_FORMATTER  # noqa: F401

DEFAULT_WORKERS = 2

WORKERS = int(os.environ.get('WORKERS', DEFAULT_WORKERS))

USE_PROXY = (os.environ.get('USE_PROXY') == 'True')
PROXY_USER = os.environ.get('PROXY_USER')
PROXY_PASS = os.environ.get('PROXY_PASS')
PROXY_LOAD_URL = os.environ.get('PROXY_LOAD_URL')
