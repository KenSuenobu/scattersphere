#!/usr/bin/env bash
#
# Script used to build Scattersphere project

set -e
mvn clean install -U -DskipTests -DskipITs

