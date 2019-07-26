FROM python:3.7
ARG ENVIRONMENT="production"

ENV LOG_LEVEL=INFO \
    RAVEN_DSN="" \
    JSEARCH_SYNC_PARALLEL="10" \
    JSEARCH_MAIN_DB="postgres://postgres:postgres@main_db/jsearch_main" \
    JSEARCH_RAW_DB="postgres://postgres:postgres@raw_db/jsearch_raw" \
    JSEARCH_CONTRACTS_API="http://contracts:8080" \
    JSEARCH_COMPILER_API="http://compiler" \
    JSEARCH_API_ENABLE_RESET_LOGS_PROCESSING="1" \
    ETH_NODE_URL="https://main-node.jwallet.network" \
    KAFKA_BOOTSTRAP_SERVERS="kafka:9092" \
    SYNCER_BACKOFF_MAX_TRIES="5" \
    PENDING_SYNCER_BACKOFF_MAX_TRIES="5" \
    DOCKERIZE_VERSION="v0.6.1" \
    PORT="8080" \
    NO_JSON_FORMATTER="0" \
    NOTABLE_ACCOUNT_UPDATE_IF_EXISTS="1" \
    ETH_BALANCE_BLOCK_OFFSET="6" \
    ETH_NODE_BATCH_REQUEST_SIZE="20" \
    ETH_NODE_ID="0x83f47b4ec7fc8a709e649df7fd2a77d34119dbd0a2e47b5430e85033108142e9" \
    PENDING_TX_BATCH_SIZE="300" \
    PENDING_TX_SLEEP_ON_NO_TXS="1" \
    POST_PROCESSING_API_PORT="8080" \
    SYNCER_API_PORT="8080" \
    WALLET_WORKER_API_PORT="8080" \
    WORKER_API_PORT="8080" \
    NOTABLES_WORKER_API_PORT="8080" \
    MONITOR_OFFSET="6"

RUN groupadd -g 999 app \
 && useradd -r -u 999 -g app app \
 && mkdir -p /app \
 && chown -R app:app /app

WORKDIR /app

RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

COPY --chown=app:app ./jsearch-service-bus /app/jsearch-service-bus/
RUN cd jsearch-service-bus && pip install --no-cache-dir . && cd ..

COPY --chown=app:app requirements*.txt /app/
RUN pip install --no-cache-dir -r requirements.txt $(test "$ENVIRONMENT" != "production" && echo "-r requirements-test.txt") 

COPY --chown=app:app . /app
RUN pip install --no-cache-dir .

USER app
ENTRYPOINT ["/app/run.sh"]
CMD ["app"]
