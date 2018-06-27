import os


JSEARCH_MAIN_DB = os.getenv('JSEARCH_MAIN_DB', 'postgres://localhost/jsearch_main')
JSEARCH_RAW_DB = os.getenv('JSEARCH_RAW_DB', 'postgres://localhost/jsearch_raw')
ETH_NODE_URL = os.getenv('ETH_NODE_URL', 'https://main-node.jwallet.network')
JSEARCH_CELERY_BROKER = os.getenv('JSEARCH_CELERY_BROKER', 'redis://localhost:6379/0')
JSEARCH_CELERY_BACKEND = os.getenv('JSEARCH_CELERY_BACKEND', 'redis://localhost:6379/0')
