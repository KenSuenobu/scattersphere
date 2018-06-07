#!/usr/bin/env bash
#
# CircleCI script used to start a Spark instance
# This script should not be used outside of the CircleCI network; this is just for CircleCI testing.

cd ~
wget http://www-us.apache.org/dist/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz -qO ./spark-2.3.0.tar.gz
tar -xvzf ./spark-2.3.0.tar.gz
rm -f spark-2.3.0.tar.gz
mv spark-2.3.0-bin-hadoop2.7 spark-2.3.0
cd spark-2.3.0/sbin
sh ./start-master.sh -h 127.0.0.1
sh ./start-slave.sh -c 2 -m 8G spark://127.0.0.1:7077
cd -
