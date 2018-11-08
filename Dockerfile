FROM python:3.6

ENV LOG_LEVEL=INFO \
    JSEARCH_SYNC_PARALLEL="10" \
    JSEARCH_MAIN_DB="postgres://postgres:postgres@main_db/jsearch_mai" \
    JSEARCH_RAW_DB="postgres://postgres:postgres@raw_db/jsearch_raw" \
    JSEARCH_CELERY_BROKER="redis://redis/0" \
    JSEARCH_CELERY_BACKEND="redis://redis/1" \
    JSEARCH_CONTRACT_API="http://localhost:8101" \
    JSEARCH_COMPILER_API="http://localhost:8102" \
    ENH_NODE_URL="https://main-node.jwallet.network"


RUN groupadd -g 999 app \
 && useradd -r -u 999 -g app app \
 && mkdir -p /app \
 && chown -R app:app /app

WORKDIR /app

COPY --chown=app:app requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=app:app py-solc /app/py-solc
RUN cd py-solc \
 && pip install --no-cache-dir .

COPY --chown=app:app docker/app /app
RUN pip install --no-cache-dir .

USER app
ENTRYPOINT ["/app/run.sh"]
CMD ["app"]
