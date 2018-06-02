#!/usr/bin/env bash
#
# Script to test Scattersphere project

set -e
HADOOP_HOME="/" mvn test $@
