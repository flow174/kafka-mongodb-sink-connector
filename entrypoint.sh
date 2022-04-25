#!/bin/bash

set -e

echo "START kafka to mongodb sink connector"

java -server -Xms6g -Xmx6g  -XX:NewRatio=3 -Dlog4j.configurationFile=./log4j2.xml -cp ./app.jar org.apache.kafka.connect.cli.ConnectStandalone ./kafka.properties ./connector.properties

echo "END Running app on `date`"