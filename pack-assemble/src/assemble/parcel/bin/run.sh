#!/bin/bash
set -e
set -x

if [ ! -z ${TEST_HDFS+x} ]; then
  set +x
  for f in /pack/lib-test/*
  do
    if [ -z ${HADOOP_CLASSPATH+x} ]; then
      export HADOOP_CLASSPATH="${f}"
    else
      export HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${f}"
    fi
  done
  set -x
  echo $HADOOP_CLASSPATH
  java -Xmx256m -Xms256m -cp ${HADOOP_CLASSPATH} pack.block.blockstore.hdfs.HdfsMiniCluster /test-hdfs >hdfs_stdout 2>hdfs_stderr &
  while [ ! -f "/test-hdfs/conf/hdfs-site.xml" ] ; do
    echo "waiting for hdfs to start..."
    sleep 1
  done
  export HDFS_CONF_DIR=/test-hdfs/conf/
fi

set +x
export CLASSPATH="${HDFS_CONF_DIR}"
set +x
for f in ${PACK_PARCEL_PATH}/lib/*
do
  export CLASSPATH="${CLASSPATH}:${f}"
done
set -x
echo $CLASSPATH

if [ ! -z ${TEST_ZK+x} ]; then
  java -Xmx256m -Xms256m pack.zk.utils.ZkMiniCluster /test-zk >zk_stdout 2>zk_stderr &
  export PACK_ZOOKEEPER_CONNECTION_STR="127.0.0.1:21810/pack"
fi

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

if [ -z ${JAVA_HOME+x} ] ; then
  JAVA_CMD="java"
else
  JAVA_CMD="${JAVA_HOME}/bin/java"
fi

case $CMD in
  (pack)
    exec -a pack ${JAVA_CMD} -Xmx256m -Xms256m pack.block.server.BlockPackServer
    ;;
  (compaction)
    exec -a pack-compactor ${JAVA_CMD} -Xmx256m -Xms256m pack.block.blockstore.compactor.PackCompactorServer
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac
