#!/bin/sh

if [ "${ENABLE_HEALTHCHECK}" = "1" ]; then
    curl -A "DockerHealthCheck" -sS "http://localhost:${PORT}/healthcheck" || exit 1
fi