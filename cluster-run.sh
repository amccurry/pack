#!/bin/bash
set -e
export CLASSPATH="${PWD}/hadoop-conf/"
for f in ${PWD}/pack-assemble/target/PACK-18*/PACK-18*/lib/*
do
  export CLASSPATH="${CLASSPATH}:${f}"
done
echo $CLASSPATH
DEBUG="-Xrunjdwp:transport=dt_socket,address=9001,server=y,suspend=y"
set -x
sudo \
 PACK_HDFS_KERBEROS_PRINCIPAL_NAME=sigma/starbuck.pdev.sigma.dsci@SIGMA.DSCI \
 PACK_HDFS_KERBEROS_KEYTAB=/root/sigma.keytab \
 PACK_LOCAL="/var/lib/pack-block" \
 PACK_SCOPE="global" \
 PACK_HDFS_PATH="/user/sigma/block-volumes-test" \
 PACK_ZOOKEEPER_CONNECTION_STR="c-sao-1.pdev.sigma.dsci/testpack" \
 java ${DEBUG} -Xmx1g -Xms1g -Ddocker.unix.socket=/var/run/docker.sock -cp ${CLASSPATH} pack.block.server.BlockPackServer
