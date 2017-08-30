#!/bin/bash
set -e
set +x
export CLASSPATH="${HDFS_CONF_DIR}"
set +x
for f in ${PACK_PARCEL_PATH}/lib/*
do
  export CLASSPATH="${CLASSPATH}:${f}"
done
set -x
echo $CLASSPATH

if [ -z ${HDFS_CONF_DIR+x} ]; then
 echo "HDFS_CONF_DIR is not defined";
else
 echo "HDFS_CONF_DIR=${HDFS_CONF_DIR}";
fi

if [ -z ${PACK_HDFS_KERBEROS_KEYTAB+x} ]; then
 echo "PACK_HDFS_KERBEROS_KEYTAB is not defined";
else
 echo "PACK_HDFS_KERBEROS_KEYTAB=${PACK_HDFS_KERBEROS_KEYTAB}";
fi

if [ -z ${PACK_HDFS_KERBEROS_PRINCIPAL_NAME+x} ]; then
 echo "PACK_HDFS_KERBEROS_PRINCIPAL_NAME is not defined";
else
 echo "PACK_HDFS_KERBEROS_PRINCIPAL_NAME=${PACK_HDFS_KERBEROS_PRINCIPAL_NAME}";
fi

if [ -z ${PACK_HDFS_PATH+x} ]; then
 echo "PACK_HDFS_PATH is not defined";
else
 echo "PACK_HDFS_PATH=${PACK_HDFS_PATH}";
fi

if [ -z ${PACK_HDFS_USER+x} ]; then
 echo "PACK_HDFS_USER is not defined";
else
 echo "PACK_HDFS_USER=${PACK_HDFS_USER}";
fi

if [ -z ${PACK_LOCAL+x} ]; then
 echo "PACK_LOCAL is not defined";
else
 echo "PACK_LOCAL=${PACK_LOCAL}";
fi

if [ -z ${PACK_SCOPE+x} ]; then
 echo "PACK_SCOPE is not defined";
else
 echo "PACK_SCOPE=${PACK_SCOPE}";
fi

if [ -z ${PACK_LOG4J_CONFIG+x} ]; then
 echo "PACK_LOG4J_CONFIG is not defined";
else
 echo "PACK_LOG4J_CONFIG=${PACK_LOG4J_CONFIG}";
fi

if [ -z ${PACK_ZOOKEEPER_CONNECTION_STR+x} ]; then
 echo "PACK_ZOOKEEPER_CONNECTION_STR is not defined";
else
 echo "PACK_ZOOKEEPER_CONNECTION_STR=${PACK_ZOOKEEPER_CONNECTION_STR}";
fi

CMD=$1

case $CMD in
  (pack)
    exec -a pack java -Xmx1g -Xms1g -cp ${CLASSPATH} pack.block.server.BlockPackServer
    ;;
  (compaction)
    exec -a pack-compactor java -Xmx1g -Xms1g -cp ${CLASSPATH} pack.block.blockstore.compactor.PackCompactorServer
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac
