import os

import pathlib
import socket

BASE_DIR = pathlib.Path(__file__).parent.parent

VERSION_FILE = BASE_DIR / 'jsearch' / 'version.txt'
VERSION = VERSION_FILE.read_text()

JSEARCH_MAIN_DB = os.getenv('JSEARCH_MAIN_DB', 'postgres://localhost/jsearch_main')
JSEARCH_RAW_DB = os.getenv('JSEARCH_RAW_DB', 'postgres://localhost/jsearch_raw')

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
NO_JSON_FORMATTER = bool(int(os.getenv('NO_JSON_FORMATTER', '0')))

NOTABLE_ACCOUNT_UPDATE_IF_EXISTS = bool(int(os.getenv('NOTABLE_ACCOUNT_UPDATE_IF_EXISTS', '1')))

# can get list of connection.
# examples:
# kafka-1:19092, kafka-2:192092
# kafka:9092
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092,')
if ',' in KAFKA_BOOTSTRAP_SERVERS:
    KAFKA_BOOTSTRAP_SERVERS = [server.strip() for server in KAFKA_BOOTSTRAP_SERVERS.split(',')]

ETH_BALANCE_BLOCK_OFFSET = os.getenv('ETH_BALANCE_BLOCK_OFFSET', 6)

ETH_NODE_URL = os.getenv('ETH_NODE_URL', 'https://main-node.jwallet.network')
ETH_NODE_BATCH_REQUEST_SIZE = int(os.getenv('ETH_NODE_BATCH_REQUEST_SIZE', '20'))

ETH_NODE_ID = os.getenv('ETH_NODE_ID', '0x83f47b4ec7fc8a709e649df7fd2a77d34119dbd0a2e47b5430e85033108142e9')
# we hardcode this node id, because we have not logic make switch between nodes.
# but still we need to think about such logic implementation

JSEARCH_CONTRACTS_API = os.getenv('JSEARCH_CONTRACTS_API', 'http://localhost:8100')
JSEARCH_COMPILER_API = os.getenv('JSEARCH_COMPILER_API', 'http://localhost:8101')
JSEARCH_SYNC_PARALLEL = int(os.getenv('JSEARCH_SYNC_PARALLEL', '10'))

PENDING_TX_BATCH_SIZE = int(os.getenv('PENDING_TX_BATCH_SIZE', '300'))
PENDING_TX_SLEEP_ON_NO_TXS = int(os.getenv('PENDING_TX_SLEEP_ON_NO_TXS', '1'))

ENABLE_RESET_POST_PROCESSING = bool(os.getenv('JSEARCH_API_ENABLE_RESET_LOGS_PROCESSING', True))
RAVEN_DSN = os.getenv('RAVEN_DSN')

SERVICE_BUS_WORKER_NAME = 'jsearch_backend'

API_QUERY_ARRAY_MAX_LENGTH = 25

HTTP_USER_AGENT = f'jsearch-backend/{VERSION} {socket.gethostname()}'.replace('\n', '')

HEALTH_LOOP_TASKS_COUNT_THRESHOLD = 10000

POST_PROCESSING_API_PORT = int(os.getenv('POST_PROCESSING_API_PORT', 8080))
SYNCER_API_PORT = int(os.getenv('SYNCER_API_PORT', 8080))
WALLET_WORKER_API_PORT = int(os.getenv('WALLET_WORKER_API_PORT', 8080))
WORKER_API_PORT = int(os.getenv('WORKER_API_PORT', 8080))
NOTABLES_WORKER_API_PORT = int(os.getenv('NOTABLES_WORKER_API_PORT', 8080))
