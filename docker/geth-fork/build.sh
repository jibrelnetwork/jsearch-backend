#!/bin/sh

git clone git@github.com:jibrelnetwork/go-ethereum.git \
  --branch feature/postgres-db \
  --depth 1

docker build . -t jsearch/geth-fork
