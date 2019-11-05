from prometheus_client import Counter, Gauge, Histogram

from jsearch import settings


# API.

METRIC_API_LOOP_TASKS_TOTAL = Gauge(
    settings.METRIC_API_LOOP_TASKS_TOTAL,
    'Total amount of tasks in the event loop.',
)
METRIC_API_REQUESTS_ORPHANED_TOTAL = Counter(
    settings.METRIC_API_REQUESTS_ORPHANED_TOTAL,
    'Total amount of requests failed due to data inconsistency.',
    ['endpoint'],
)
METRIC_API_REQUESTS_LATENCY_SECONDS = Histogram(
    settings.METRIC_API_REQUESTS_LATENCY_SECONDS,
    'Time spent to serve response in seconds.',
    ['endpoint'],
)
METRIC_API_REQUESTS_IN_PROGRESS_TOTAL = Gauge(
    settings.METRIC_API_REQUESTS_IN_PROGRESS_TOTAL,
    'Total amount of requests in progress.',
    ['endpoint', 'method'],
)
METRIC_API_REQUESTS_TOTAL = Counter(
    settings.METRIC_API_REQUESTS_TOTAL,
    'Total amount of served requests.',
    ['endpoint', 'method', 'status'],
)


# Blocks Syncer.

METRIC_SYNCER_LAG_ETHERSCAN = Gauge(
    settings.METRIC_LAG_ETHERSCAN,
    'Chain lag with Etherscan',
)
METRIC_SYNCER_LAG_INFURA = Gauge(
    settings.METRIC_LAG_INFURA,
    'Chain lag with Infura',
)
METRIC_SYNCER_LAG_JWALLET = Gauge(
    settings.METRIC_LAG_JWALLET,
    'Chain lag with jWallet',
)
METRIC_SYNCER_LOOP_TASKS_TOTAL = Gauge(
    settings.METRIC_SYNCER_LOOP_TASKS_TOTAL,
    'Total amount of tasks in the event loop.',
)


# Pending TXs Syncer.

METRIC_SYNCER_PENDING_LOOP_TASKS_TOTAL = Gauge(
    settings.METRIC_SYNCER_PENDING_LOOP_TASKS_TOTAL,
    'Total amount of tasks in the event loop.',
)
