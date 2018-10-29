#!/bin/bash -e

RUNMODE="${1:-app}"

export APP_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Starting jsearch-backend service, mode: '${RUNMODE}', version: `cat /app/version.txt` on node `hostname`"

if [ "${RUNMODE}" = "app" ]; then
    python manage.py upgrade head
    gunicorn --bind 0.0.0.0:8000 jsearch.api.app:make_app --worker-class aiohttp.worker.GunicornWebWorker
elif [ "${RUNMODE}" = "syncer" ]; then
    jsearch-syncer
elif [ "${RUNMODE}" = "celerybeat" ]; then
    celery beat -A jsearch.common.celery -l info
elif [ "${RUNMODE}" = "celeryworker" ]; then
    celery worker -A jsearch.common.celery -l info
else
    echo "Wrong RUNMODE supplied, exiting"
    exit 1
fi
