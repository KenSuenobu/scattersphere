#!/usr/bin/env bash
#
# CircleCI script used to start a Spark instance
# This script should not be used outside of the CircleCI network; this is just for CircleCI testing.

docker pull ksuenobu/spark-latest
DOCKER_ID=$(docker run -d -it --net host --expose 8080 --expose 8081 --expose 7077 --expose 4040 ksuenobu/spark-latest start-master.sh)

sleep 3
echo "Spark started as ID ${DOCKER_ID}"

echo "${DOCKER_ID}" > /tmp/docker-id
