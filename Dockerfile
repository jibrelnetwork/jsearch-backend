FROM python:3.6

ARG ENVIRONMENT="production"

ENV ENVIRONMENT=${ENVIRONMENT} \
    LOG_LEVEL=INFO \
    RAVEN_DSN="" \
    JSEARCH_SYNC_PARALLEL="10" \
    JSEARCH_MAIN_DB="postgres://postgres:postgres@main_db/jsearch_main" \
    JSEARCH_RAW_DB="postgres://postgres:postgres@raw_db/jsearch_raw" \
    JSEARCH_CELERY_BROKER="redis://redis/0" \
    JSEARCH_CELERY_BACKEND="redis://redis/1" \
    JSEARCH_CONTRACTS_API="http://contracts:8080" \
    JSEARCH_COMPILER_API="http://compiler" \
    JSEARCH_API_ENABLE_RESET_LOGS_PROCESSING="1" \
    ENH_NODE_URL="https://main-node.jwallet.network" \
    KAFKA_BOOTSTRAP_SERVERS="" \
    POETRY_VERSION="0.12.12"

RUN groupadd -g 999 app \
 && useradd -r -u 999 -g app app \
 && mkdir -p /app \
 && chown -R app:app /app

WORKDIR /app

RUN pip install --no-cache-dir poetry==$POETRY_VERSION \
 && poetry config settings.virtualenvs.create false

COPY --chown=app:app pyproject.toml poetry.lock /app/
COPY --chown=app:app ./jsearch-service-bus /app/jsearch-service-bus/

RUN poetry install $(test "$ENVIRONMENT" = production && echo "--no-dev") --no-interaction --no-ansi

COPY --chown=app:app . /app
RUN pip install --no-cache-dir --editable .

USER app
ENTRYPOINT ["/app/run.sh"]
CMD ["app"]
