import os

import pathlib
import socket

BASE_DIR = pathlib.Path(__file__).parent.parent

VERSION = '0.1.3'

JSEARCH_MAIN_DB = os.getenv('JSEARCH_MAIN_DB', 'postgres://localhost/jsearch_main')
JSEARCH_RAW_DB = os.getenv('JSEARCH_RAW_DB', 'postgres://localhost/jsearch_raw')

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# can get list of connection.
# examples:
# kafka-1:19092, kafka-2:192092
# kafka:9092
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092,')
if ',' in KAFKA_BOOTSTRAP_SERVERS:
    KAFKA_BOOTSTRAP_SERVERS = [server.strip() for server in KAFKA_BOOTSTRAP_SERVERS.split(',')]

ETH_BALANCE_BLOCK_OFFSET = os.getenv('ETH_BALANCE_BLOCK_OFFSET', 6)

ETH_NODE_URL = os.getenv('ETH_NODE_URL', 'https://main-node.jwallet.network')
ETH_NODE_BATCH_REQUEST_SIZE = int(os.getenv('ETH_NODE_BATCH_REQUEST_SIZE', '50'))

JSEARCH_CELERY_BROKER = os.getenv('JSEARCH_CELERY_BROKER', 'redis://localhost:6379/0')
JSEARCH_CELERY_BACKEND = os.getenv('JSEARCH_CELERY_BACKEND', 'redis://localhost:6379/0')
JSEARCH_CONTRACTS_API = os.getenv('JSEARCH_CONTRACTS_API', 'http://localhost:8100')
JSEARCH_COMPILER_API = os.getenv('JSEARCH_COMPILER_API', 'http://localhost:8101')
JSEARCH_SYNC_PARALLEL = int(os.getenv('JSEARCH_SYNC_PARALLEL', '10'))

ENABLE_RESET_POST_PROCESSING = bool(os.getenv('JSEARCH_API_ENABLE_RESET_LOGS_PROCESSING', True))
RAVEN_DSN = os.getenv('RAVEN_DSN')

SERVICE_BUS_WORKER_NAME = 'jsearch_backend'

API_QUERY_ARRAY_MAX_LENGTH = 25

HTTP_USER_AGENT = f'jsearch-backend/{VERSION} {socket.gethostname()}'

LAST_BLOCK_OFFSET = int(os.getenv('BLOCKCHAIN_OFFSET', 6))
HEALTH_LOOP_TASKS_COUNT_THRESHOLD = 10000

POST_PROCESSING_API_PORT = int(os.getenv('POST_PROCESSING_API_PORT', 8080))
