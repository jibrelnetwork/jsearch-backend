from prometheus_client import Counter, Gauge, Histogram

METRIC_API_LOOP_TASKS_TOTAL = Gauge(
    'jsearch_api_loop_tasks_total',
    'Total amount of tasks in the event loop.',
)
METRIC_API_REQUESTS_ORPHANED_TOTAL = Counter(
    'jsearch_api_requests_orphaned_total',
    'Total amount of requests failed due to data inconsistency.',
    ['endpoint'],
)
METRIC_API_REQUESTS_LATENCY_SECONDS = Histogram(
    'jsearch_api_requests_latency_seconds',
    'Time spent to serve response in seconds.',
    ['endpoint'],
)
METRIC_API_REQUESTS_IN_PROGRESS_TOTAL = Gauge(
    'jsearch_api_requests_in_progress_total',
    'Total amount of requests in progress.',
    ['endpoint', 'method'],
)
METRIC_API_REQUESTS_TOTAL = Counter(
    'jsearch_api_requests_total',
    'Total amount of served requests.',
    ['endpoint', 'method', 'status'],
)


# Blocks Syncer.

METRIC_SYNCER_EVENT_SYNC_DURATION = Histogram(
    'jsearch_syncer_chain_event_sync_duration_seconds',
    'Time spent to process chain events in seconds.',
    ['event_type'],
)
METRIC_SYNCER_LAG_ETHERSCAN = Gauge(
    'jsearch_syncer_lag_etherscan',
    'Chain lag with Etherscan',
)
METRIC_SYNCER_LAG_INFURA = Gauge(
    'jsearch_syncer_lag_infura',
    'Chain lag with Infura',
)
METRIC_SYNCER_LAG_JWALLET = Gauge(
    'jsearch_syncer_lag_jwallet',
    'Chain lag with jWallet',
)
METRIC_SYNCER_LOOP_TASKS_TOTAL = Gauge(
    'jsearch_syncer_loop_tasks_total',
    'Total amount of tasks in the event loop.',
)


# Pending TXs Syncer.

METRIC_SYNCER_PENDING_LOOP_TASKS_TOTAL = Gauge(
    'jsearch_syncer_pending_loop_tasks_total',
    'Total amount of tasks in the event loop.',
)
METRIC_SYNCER_PENDING_TXS_BATCH_SYNC_SPEED = Histogram(
    'jsearch_syncer_pending_txs_per_second',
    'Syncing speed of pending TXs.',
)
METRIC_SYNCER_PENDING_LAG_RAW_DB = Gauge(
    'jsearch_syncer_pending_txs_lag',
    'Left to load pending transactions'
)
