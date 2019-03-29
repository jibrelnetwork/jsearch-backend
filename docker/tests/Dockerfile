FROM jsearch/backend

ENV JSEARCH_MAIN_DB_TEST=postgres://postgres:posgres@test_db/jsearch_main_test

USER root
COPY --chown=app:app requirements-test.txt /app/
RUN pip install --no-cache-dir -r requirements-test.txt

ENTRYPOINT ["pytest"]
CMD ["-s"]
