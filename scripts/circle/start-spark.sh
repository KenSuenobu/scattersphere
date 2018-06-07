#!/usr/bin/env bash
#
# CircleCI script used to start a Spark instance
# This script should not be used outside of the CircleCI network; this is just for CircleCI testing.

wget http://www-us.apache.org/dist/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz -qO /tmp/spark-2.3.0.tar.gz
cd /opt ; tar -xvzf /tmp/spark-2.3.0.tar.gz
rm -f /tmp/spark-2.3.0.tar.gz
cd /opt ; mv spark-* apache-spark-2.3.0
cd /opt/apache-spark-2.3.0/sbin ; sh ./start-master.sh
