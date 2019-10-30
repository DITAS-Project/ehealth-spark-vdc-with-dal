#! /bin/bash

function check {
    [ -z "$2" ] && echo "Need to set $1" && exit 1;
}

check MINIO_URI $MINIO_URI
check MINIO_ACCESS_KEY $MINIO_ACCESS_KEY
check MINIO_SECRET_KEY $MINIO_SECRET_KEY
check MYSQL_URI $MYSQL_URI
check MYSQL_USERNAME $MYSQL_USERNAME
check MYSQL_PASSWORD $MYSQL_PASSWORD
check POLICY_ENFORCEMENT_URI $POLICY_ENFORCEMENT_URI
check KEYCLOAK_PUBLIC_KEY_URI $KEYCLOAK_PUBLIC_KEY_URI

MYSQL_URI=$(echo $MYSQL_URI | sed -e "s,&,\\\&,")

sed -e "s,#{MINIO_URI},$MINIO_URI,g" \
    -e "s,#{MINIO_ACCESS_KEY},$MINIO_ACCESS_KEY,g" \
    -e "s,#{MINIO_SECRET_KEY},$MINIO_SECRET_KEY,g" \
    -e "s,#{MYSQL_URI},$MYSQL_URI,g" \
    -e "s,#{MYSQL_USERNAME},$MYSQL_USERNAME,g" \
    -e "s,#{MYSQL_PASSWORD},$MYSQL_PASSWORD,g" \
    -e "s,#{POLICY_ENFORCEMENT_URI},$POLICY_ENFORCEMENT_URI,g" \
    -e "s,#{KEYCLOAK_PUBLIC_KEY_URI},$KEYCLOAK_PUBLIC_KEY_URI,g" \
    /app/config/ehealth-dal-config.yml.sed > /app/config/ehealth-dal-config.yml

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

/app/dist/ehealth-dal-0.1/bin/ehealth-server /app/config/ehealth-dal-config.yml &

child=$!
wait "$child"
