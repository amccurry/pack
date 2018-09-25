#!/bin/bash
set -e
mkdir -p ./hadoop-conf-local/
export CLASSPATH="${PWD}/hadoop-conf-local/"
for f in ${PWD}/pack-assemble/target/PACK-18*/PACK-18*/lib/*
do
  export CLASSPATH="${CLASSPATH}:${f}"
done
echo $CLASSPATH
set -x
sudo \
 PACK_LOCAL="/var/lib/pack-block" \
 PACK_SCOPE="global" \
 PACK_HDFS_PATH="/pack" \
  java ${DEBUG} -Xmx1g -Xms1g -Ddocker.unix.socket=/var/run/docker.sock -cp ${CLASSPATH} pack.block.server.BlockPackServer
