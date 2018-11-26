FROM jsearch/geth-fork

FROM jsearch/api

ENV JSEARCH_MAIN_DB_TEST=postgres://postgres:posgres@test_db/jsearch_main_test

COPY --chown=app:app --from=0 /geth /usr/local/bin/geth

USER root
COPY --chown=app:app requirements-test.txt /app/
RUN pip install --no-cache-dir -r requirements-test.txt

ENTRYPOINT ["pytest"]
CMD ["-s"]
