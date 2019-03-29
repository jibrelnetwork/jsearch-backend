#FROM jsearch/geth-fork as geth_fork
#FROM jsearch/geth AS geth
FROM jsearch/backend

ENV JSEARCH_MAIN_DB_TEST=postgres://postgres:posgres@test_db/jsearch_main_test

#COPY --chown=app:app --from=geth_fork /geth /usr/local/bin/geth-fork
#COPY --chown=app:app --from=geth /geth /usr/local/bin/geth

USER root
COPY --chown=app:app requirements-test.txt /app/
RUN pip install --no-cache-dir -r requirements-test.txt

ENTRYPOINT ["pytest"]
CMD ["-s"]
