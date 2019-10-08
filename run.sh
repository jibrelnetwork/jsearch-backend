#!/bin/bash -e

echo "Starting Jsearch Backend service, mode '$1' version: `cat /app/version.txt` on node `hostname`"
echo "Supported commands:
  jsearch-syncer
  jsearch-syncer-pending
  app
  migrate
"

RUNMODE="${1:-app}"

echo "Run: ${RUNMODE}
"

wait_raw_db_ready () {
    dockerize -wait tcp://`python -c 'import dsnparse; p = dsnparse.parse_environ("JSEARCH_RAW_DB", hostname="localhost", port="5432"); print(p.hostloc)'`
}
wait_main_db_ready () {
    dockerize -wait tcp://`python -c 'import dsnparse; p = dsnparse.parse_environ("JSEARCH_MAIN_DB", hostname="localhost", port="5432"); print(p.hostloc)'`
}


if [[ "${RUNMODE}" = "jsearch-syncer" ]]; then
    wait_raw_db_ready
    wait_main_db_ready
elif [[ "${RUNMODE}" = "jsearch-syncer-pending" ]]; then
    wait_raw_db_ready
    wait_main_db_ready
elif [[ "${RUNMODE}" = "app" ]]; then
    wait_main_db_ready
elif [[ "${RUNMODE}" = "migrate" ]]; then
    wait_main_db_ready
fi


if [[ "$@" = "app" ]]; then
    python manage.py apply_if_db_is_empty
    gunicorn -c gunicorn-conf.py jsearch.api.app:make_app
elif [[ "$@" = "migrate" ]]; then
    python manage.py upgrade head
else
    exec "$@"
fi
