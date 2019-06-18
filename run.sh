#!/bin/bash -e

echo "Starting Jsearch Backend service, mode '$1' version: `cat /app/version.txt` on node `hostname`"


wait_kafka_ready () {
    # Collect Kafka nodes and wait for them to be up.
    KAFKA_LIST=`echo ${KAFKA_BOOTSTRAP_SERVERS} | tr "," "\n"`
    KAFKA_WAIT=""

    for x in ${KAFKA_LIST}
    do
        KAFKA_WAIT="${KAFKA_WAIT} -wait tcp://${x}"
    done

    dockerize ${KAFKA_WAIT}
}

wait_raw_db_ready () {
    dockerize -wait tcp://`python -c 'import dsnparse; p = dsnparse.parse_environ("JSEARCH_RAW_DB", hostname="localhost", port="5432"); print(p.hostloc)'`
}
wait_main_db_ready () {
    dockerize -wait tcp://`python -c 'import dsnparse; p = dsnparse.parse_environ("JSEARCH_MAIN_DB", hostname="localhost", port="5432"); print(p.hostloc)'`
}


if [[ "$1" = "jsearch-syncer" ]]; then
    wait_kafka_ready
    wait_raw_db_ready
    wait_main_db_ready
elif [[ "$1" = "jsearch-syncer-pending" ]]; then
    wait_raw_db_ready
    wait_main_db_ready
elif [[ "$1" = "jsearch-post-processing" ]]; then
    wait_kafka_ready
    wait_main_db_ready
elif [[ "$1" = "jsearch-worker" ]]; then
    wait_kafka_ready
    wait_main_db_ready
elif [[ "$1" = "jsearch-wallet-worker" ]]; then
    wait_kafka_ready
    wait_main_db_ready
elif [[ "$1" = "jsearch-notable-accounts-worker" ]]; then
    wait_kafka_ready
    wait_main_db_ready
elif [[ "$1" = "app" ]]; then
    wait_main_db_ready
fi


if [[ "$@" = "app" ]]; then
    python manage.py upgrade head
    gunicorn -c gunicorn-conf.py jsearch.api.app:make_app
else
    exec "$@"
fi
