#!/bin/bash
set -e
mkdir -p ./hadoop-conf-local/
export CLASSPATH="${PWD}/hadoop-conf-local/"
for f in ${PACK_LIB}/*
do
  export CLASSPATH="${CLASSPATH}:${f}"
done
echo $CLASSPATH
set -x
sudo \
 PACK_LOCAL="/var/lib/pack" \
 PACK_SCOPE="global" \
 PACK_HDFS_PATH="/pack" \
  java ${DEBUG} -Xmx1g -Xms1g -cp ${CLASSPATH} pack.block.blockstore.compactor.PackCompactorServer
