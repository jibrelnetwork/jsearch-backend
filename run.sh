#!/bin/bash -e

RUNMODE="${1:-app}"

export APP_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Starting jsearch-backend service, mode: '${RUNMODE}', version: `cat /app/version.txt` on node `hostname`"

if [ "${RUNMODE}" = "app" ]; then
    python manage.py upgrade head
    gunicorn -c gunicorn-conf.py jsearch.api.app:make_app
elif [ "${RUNMODE}" = "syncer" ]; then
    jsearch-syncer
else
    echo "Executing script: $@"
    exec "$@"
fi
