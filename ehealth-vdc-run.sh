#! /bin/bash

function check {
    [ -z "$2" ] && echo "Need to set $1" && exit 1;
}

check PLAY_HTTP_SECRET $PLAY_HTTP_SECRET
check DAL_URI $DAL_URI
check DAL_PORT $DAL_PORT

_term() {
  echo "Caught SIGTERM signal!"
  kill -TERM "$child" 2>/dev/null
}

_int() {
  echo "Caught SIGINT signal!"
  kill -INT "$child" 2>/dev/null
}

trap _term SIGTERM
trap _int SIGINT

/app/dist/ehealth-sample-spark-vdc-1.0/bin/ehealth-sample-spark-vdc \
    -Dplay.http.secret.key=$PLAY_HTTP_SECRET \
    -Ddal.url=$DAL_URI \
    -Ddal.port=$DAL_PORT &

child=$!
wait "$child"
