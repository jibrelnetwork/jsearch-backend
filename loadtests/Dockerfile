FROM python:3.6

ENV JSEARCH_MAIN_DB="postgres://postgres@main_db/postgres"

RUN groupadd -g 999 app \
 && useradd -r -u 999 -g app app \
 && mkdir -p /app \
 && chown -R app:app /app

WORKDIR /app

COPY --chown=app:app requirements.txt locustfile.py /app/
RUN pip install --no-cache-dir -r requirements.txt

USER app
ENTRYPOINT ["locust", "-f", "/app/locustfile.py"]

