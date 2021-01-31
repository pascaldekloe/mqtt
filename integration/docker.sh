#!/bin/sh
set -e

cd /code

go install ./cmd/mqttc
for host in $MQTT_HOSTS
do
  until mqttc $host
  do
    echo "MQTT on host $host unavailable; retry in a second…"
    sleep 1
  done
done

exec go test -v -count 10 -race ./integration
