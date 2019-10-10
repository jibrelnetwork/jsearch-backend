import os

from jsearch.settings import *

DEFAULT_WORKERS = 2

JSEARCH_MAIN_DB = os.environ['JSEARCH_MAIN_DB']

WORKERS = int(os.environ.get('WORKERS', DEFAULT_WORKERS))

USE_PROXY = (os.environ.get('USE_PROXY') == 'True')
PROXY_USER = os.environ.get('PROXY_USER')
PROXY_PASS = os.environ.get('PROXY_PASS')
PROXY_LIST_PATH = os.path.join(os.path.dirname(__file__), 'proxy.list')