#!/bin/sh

if [ "${ENABLE_HEALTHCHECK}" = "1" ]; then
    [ $(curl -o -I -L -s -w "%{http_code}" "http://localhost:${PORT}/healthcheck") -eq 200 ] || exit 1
fi
