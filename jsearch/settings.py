import os

import pathlib
import socket

BASE_DIR = pathlib.Path(__file__).parent.parent

VERSION_FILE = BASE_DIR / 'jsearch' / 'version.txt'
VERSION = VERSION_FILE.read_text()

JSEARCH_MAIN_DB = os.environ['JSEARCH_MAIN_DB']
JSEARCH_RAW_DB = os.environ['JSEARCH_RAW_DB']

LOG_LEVEL = os.environ['LOG_LEVEL']
NO_JSON_FORMATTER = bool(int(os.environ['NO_JSON_FORMATTER']))

ETH_BALANCE_BLOCK_OFFSET = os.environ['ETH_BALANCE_BLOCK_OFFSET']

ETH_NODE_URL = os.environ['ETH_NODE_URL']
ETH_NODE_BATCH_REQUEST_SIZE = int(os.environ['ETH_NODE_BATCH_REQUEST_SIZE'])

# we hardcode this node id, because we have not logic make switch between nodes.
# but still we need to think about such logic implementation
ETH_NODE_ID = os.environ['ETH_NODE_ID']

JSEARCH_CONTRACTS_API = os.environ['JSEARCH_CONTRACTS_API']
JSEARCH_COMPILER_API = os.environ['JSEARCH_COMPILER_API']

PENDING_TX_BATCH_SIZE = int(os.environ['PENDING_TX_BATCH_SIZE'])
PENDING_TX_SLEEP_ON_NO_TXS = int(os.environ['PENDING_TX_SLEEP_ON_NO_TXS'])

SENTRY_DSN = os.environ['SENTRY_DSN']

API_QUERY_ARRAY_MAX_LENGTH = 25

HTTP_USER_AGENT = f'jsearch-backend/{VERSION} {socket.gethostname()}'.replace('\n', '')

HEALTH_LOOP_TASKS_COUNT_THRESHOLD = 10000

SYNCER_API_PORT = int(os.environ['SYNCER_API_PORT'])
SYNCER_PENDING_API_PORT = int(os.environ['SYNCER_PENDING_API_PORT'])

SYNCER_BACKOFF_MAX_TRIES = int(os.environ['SYNCER_BACKOFF_MAX_TRIES'])
PENDING_SYNCER_BACKOFF_MAX_TRIES = int(os.environ['PENDING_SYNCER_BACKOFF_MAX_TRIES'])


METRIC_API_LOOP_TASKS_TOTAL = 'jsearch_api_loop_tasks_total'
METRIC_SYNCER_LOOP_TASKS_TOTAL = 'jsearch_syncer_loop_tasks_total'
METRIC_SYNCER_PENDING_LOOP_TASKS_TOTAL = 'jsearch_syncer_pending_loop_tasks_total'
