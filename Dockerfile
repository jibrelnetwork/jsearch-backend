FROM python:3.6

ENV LOG_LEVEL=INFO

RUN groupadd -g 999 app \
 && useradd -r -u 999 -g app app \
 && mkdir -p /app \
 && chown -R app:app /app

WORKDIR /app

COPY requirements*.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY py-solc /app/py-solc
RUN cd py-solc \
 && pip install --no-cache-dir .

COPY . /app
RUN pip install --no-cache-dir .

USER app
ENTRYPOINT ["/app/run.sh"]
CMD ["app"]
