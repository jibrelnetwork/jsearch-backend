FROM ubuntu:xenial

ENV PATH=/usr/lib/go-1.9/bin:$PATH
ENV JSEARCH_RAW_DB="postgres://postgres:postgres@raw_db/jsearch_raw"

RUN apt-get update && \
  apt-get upgrade -q -y && \
  apt-get install -y --no-install-recommends golang-1.9 git make gcc libc-dev ca-certificates

RUN mkdir /sources
COPY go-ethereum /sources/go-ethereum

RUN (cd /sources/go-ethereum && make geth) && \
  cp /sources/go-ethereum/build/bin/geth /geth

RUN apt-get remove -y golang-1.9 git make gcc libc-dev && \
  apt autoremove -y && apt-get clean && \
  rm -rf /sources/go-ethereum

ENTRYPOINT ["/geth"]
