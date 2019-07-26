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

NOTABLE_ACCOUNT_UPDATE_IF_EXISTS = bool(int(os.environ['NOTABLE_ACCOUNT_UPDATE_IF_EXISTS']))

# can get list of connection.
# examples:
# kafka-1:19092, kafka-2:192092
# kafka:9092
KAFKA_BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS']
if ',' in KAFKA_BOOTSTRAP_SERVERS:
    KAFKA_BOOTSTRAP_SERVERS = [server.strip() for server in KAFKA_BOOTSTRAP_SERVERS.split(',')]

ETH_BALANCE_BLOCK_OFFSET = os.environ['ETH_BALANCE_BLOCK_OFFSET']

ETH_NODE_URL = os.environ['ETH_NODE_URL']
ETH_NODE_BATCH_REQUEST_SIZE = int(os.environ['ETH_NODE_BATCH_REQUEST_SIZE'])

ETH_NODE_ID = os.environ['ETH_NODE_ID']
# we hardcode this node id, because we have not logic make switch between nodes.
# but still we need to think about such logic implementation

JSEARCH_CONTRACTS_API = os.environ['JSEARCH_CONTRACTS_API']
JSEARCH_COMPILER_API = os.environ['JSEARCH_COMPILER_API']
JSEARCH_SYNC_PARALLEL = int(os.environ['JSEARCH_SYNC_PARALLEL'])

PENDING_TX_BATCH_SIZE = int(os.environ['PENDING_TX_BATCH_SIZE'])
PENDING_TX_SLEEP_ON_NO_TXS = int(os.environ['PENDING_TX_SLEEP_ON_NO_TXS'])

ENABLE_RESET_POST_PROCESSING = bool(os.environ['JSEARCH_API_ENABLE_RESET_LOGS_PROCESSING'])
RAVEN_DSN = os.environ['RAVEN_DSN']

SERVICE_BUS_WORKER_NAME = 'jsearch_backend'

API_QUERY_ARRAY_MAX_LENGTH = 25

HTTP_USER_AGENT = f'jsearch-backend/{VERSION} {socket.gethostname()}'.replace('\n', '')

HEALTH_LOOP_TASKS_COUNT_THRESHOLD = 10000

POST_PROCESSING_API_PORT = int(os.environ['POST_PROCESSING_API_PORT'])
SYNCER_API_PORT = int(os.environ['SYNCER_API_PORT'])
WALLET_WORKER_API_PORT = int(os.environ['WALLET_WORKER_API_PORT'])
WORKER_API_PORT = int(os.environ['WORKER_API_PORT'])
NOTABLES_WORKER_API_PORT = int(os.environ['NOTABLES_WORKER_API_PORT'])

SYNCER_BACKOFF_MAX_TRIES = int(os.environ['SYNCER_BACKOFF_MAX_TRIES'])
PENDING_SYNCER_BACKOFF_MAX_TRIES = int(os.environ['PENDING_SYNCER_BACKOFF_MAX_TRIES'])


METRIC_API_LOOP_TASKS_TOTAL = 'jsearch_api_loop_tasks_total'
METRIC_NOTABLE_ACCOUNTS_WORKER_LOOP_TASKS_TOTAL = 'jsearch_notable_accounts_worker_loop_tasks_total'
METRIC_SYNCER_LOOP_TASKS_TOTAL = 'jsearch_syncer_loop_tasks_total'
METRIC_SYNCER_PENDING_LOOP_TASKS_TOTAL = 'jsearch_syncer_pending_loop_tasks_total'
