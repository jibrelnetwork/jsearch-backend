# Build Goose in a stock Go builder container
FROM golang:1.13 as goose

# use pure go implementation to avoid dependency from glibc
ENV CGO_ENABLED=0

RUN go get -u github.com/pressly/goose/cmd/goose


FROM python:3.7
ARG ENVIRONMENT="production"


ENV LOG_LEVEL="INFO" \
    SENTRY_DSN="" \
    JSEARCH_MAIN_DB="postgres://postgres:postgres@main_db/jsearch_main" \
    JSEARCH_RAW_DB="postgres://postgres:postgres@raw_db/jsearch_raw" \
    JSEARCH_CONTRACTS_API="http://contracts:8080" \
    JSEARCH_COMPILER_API="http://compiler" \
    ETH_NODE_URL="https://main-node.jwallet.network" \
    FORK_NODES="[\"https://main-node.jwallet.network\"]" \
    SYNCER_BACKOFF_MAX_TRIES="3" \
    PENDING_SYNCER_BACKOFF_MAX_TRIES="5" \
    DOCKERIZE_VERSION="v0.6.1" \
    PORT="8080" \
    QUERY_TIMEOUT="60" \
    NO_JSON_FORMATTER="0" \
    ETH_BALANCE_BLOCK_OFFSET="6" \
    ETH_NODE_BATCH_REQUEST_SIZE="20" \
    ETH_NODE_ID="0x83f47b4ec7fc8a709e649df7fd2a77d34119dbd0a2e47b5430e85033108142e9" \
    ETH_NODE_SWITCH_TIMEOUT="120" \
    PENDING_TX_BATCH_SIZE="300" \
    PENDING_TX_SLEEP_ON_NO_TXS="1" \
    SYNCER_API_PORT="8080" \
    SYNCER_PENDING_API_PORT="8080" \
    MONITOR_OFFSET="6" \
    UPDATE_LAG_STATISTICS_DELAY_SECONDS="60" \
    SYNC_RANGE="" \
    SYNCER_WORKERS="1" \
    SYNCER_CHECK_LAG="1" \
    SYNCER_CHECK_HOLES="1" \
    SYNCER_RESYNC="0" \
    SYNCER_RESYNC_CHAIN_SPLITS="0" \
    TOKEN_HOLDERS_CLEANER_SLEEP_TIME="0.1" \
    TOKEN_HOLDERS_CLEANER_BATCH_SIZE="100" \
    TOKEN_HOLDERS_CLEANER_BLOCKS_OFFSET="6" \
    TOKEN_HOLDERS_CLEANER_QUERIES_PARALLEL="10" \
    ENABLE_HEALTHCHECK="0" \
    HEALTHCHECK_CURL_TIMEOUT="60" \
    API_PAGING_LIMIT_DEFAULT="20" \
    API_PAGING_LIMIT_MAX="100" \
    DEX_CONTRACT="" \
    PROXY_LOAD_URL="http://proxyrack.net/rotating/megaproxy/" \
    SYNCER_WAIT_MIGRATION='20200312122200' \
    API_TOKEN_HOLDER_THRESHOLD='0.008'

RUN groupadd -g 999 app \
 && useradd -r -u 999 -g app app \
 && mkdir -p /app

WORKDIR /app

RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

COPY requirements*.txt /app/
RUN pip install --no-cache-dir -r requirements.txt $(test "$ENVIRONMENT" != "production" && echo "-r requirements-test.txt") 

COPY . /app
RUN pip install --no-cache-dir .

COPY --from=goose /go/bin/goose /usr/local/bin/

USER app
ENTRYPOINT ["/app/run.sh"]
CMD ["app"]

HEALTHCHECK --start-period=30s --interval=5s --retries=3 CMD ./scripts/healthcheck.sh
