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
    ENH_NODE_URL="https://main-node.jwallet.network" \
    KAFKA_BOOTSTRAP_SERVERS="" \
    DOCKERIZE_VERSION="v0.6.1"

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
