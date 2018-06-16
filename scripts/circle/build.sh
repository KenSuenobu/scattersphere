#!/usr/bin/env bash
#
# Script used to build Scattersphere project

set -e
mvn clean package install -U -DskipTests -DskipITs

