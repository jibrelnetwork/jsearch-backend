FROM python:3.6

RUN mkdir -p /app
WORKDIR /app

COPY requirements*.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY py-solc /app/py-solc
RUN cd py-solc \
 && pip install --no-cache-dir .

COPY . /app
RUN pip install --no-cache-dir .

ENTRYPOINT ["/app/run.sh"]
CMD ["app"]
