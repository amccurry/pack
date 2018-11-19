#!/bin/bash
set -e
set -x

set +x
export CLASSPATH="${HDFS_CONF_DIR}"
set +x
for f in ${PACK_PARCEL_PATH}/lib/*
do
  export CLASSPATH="${CLASSPATH}:${f}"
done
export CLASSPATH="${CLASSPATH}:$(hadoop jar ${PACK_PARCEL_PATH}/lib/pack-block-*.jar pack.block.classpath.GetPackHadoopClasspath)"
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

if [ -z ${PACK_LOG4J_CONFIG+x} ]; then
 echo "PACK_LOG4J_CONFIG is not defined";
else
 echo "PACK_LOG4J_CONFIG=${PACK_LOG4J_CONFIG}";
fi

CMD=$1

if [ -z ${JAVA_HOME+x} ] ; then
  JAVA_CMD="java"
else
  JAVA_CMD="${JAVA_HOME}/bin/java"
fi

case $CMD in
  (server)
    exec -a pack ${JAVA_CMD} -Xmx256m -Xms256m pack.block.server.BlockPackServer
    ;;
  (compactor)
    exec -a pack-compactor ${JAVA_CMD} -Xmx256m -Xms256m pack.block.blockstore.compactor.PackCompactorServer
    ;;
  (*)
    exec -a pack-cli ${JAVA_CMD} -Xmx256m -Xms256m pack.block.blockstore.cli.PackCli "$@"
    ;;
esac
