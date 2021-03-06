#!/usr/bin/env bash
#
# CircleCI script used to start a Spark instance
# This script should not be used outside of the CircleCI network; this is just for CircleCI testing.

VERSION="2.3.2"

echo "Downloading Spark ${VERSION}"
echo

cd ~
wget http://www-us.apache.org/dist/spark/spark-${VERSION}/spark-${VERSION}-bin-hadoop2.7.tgz -qO ./spark-${VERSION}.tar.gz
tar -xvzf ./spark-${VERSION}.tar.gz
rm -f spark-${VERSION}.tar.gz
mv spark-${VERSION}-bin-hadoop2.7 spark-${VERSION}
cd spark-${VERSION}/sbin
bash ./start-master.sh -h 127.0.0.1
bash ./start-slave.sh -c 2 -m 8G spark://127.0.0.1:7077
cd -
