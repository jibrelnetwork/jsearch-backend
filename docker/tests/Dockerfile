FROM jsearch/backend

ENV JSEARCH_MAIN_DB_TEST=postgres://postgres:postgres@test_db/test
ENV JSEARCH_RAW_DB_TEST=postgres://postgres:postgres@test_db/test

USER root
COPY --chown=app:app requirements-test.txt /app/
RUN pip install --no-cache-dir -r requirements-test.txt

ENTRYPOINT ["pytest"]
CMD ["-s"]
