#!/usr/bin/env bash
#
# Shuts down Spark Container in CircleCI

docker kill $(cat /tmp/docker-id)
