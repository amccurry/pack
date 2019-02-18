#!/bin/bash
set -e
if [ -z ${PACK_DEBUG+x} ]; then
 set +x
else
 set -x
fi


export CLASSPATH="${HDFS_CONF_DIR}"

for f in ${PACK_PARCEL_PATH}/lib/* ; do
  export CLASSPATH="${CLASSPATH}:${f}"
done

export CLASSPATH="${CLASSPATH}:$(hadoop --config ${HDFS_CONF_DIR} classpath)"

if [ -z ${HDFS_CONF_DIR+x} ]; then
 echo "HDFS_CONF_DIR is not defined" >/dev/null
else
 echo "HDFS_CONF_DIR=${HDFS_CONF_DIR}" >/dev/null
fi

if [ -z ${PACK_HDFS_KERBEROS_KEYTAB+x} ]; then
 echo "PACK_HDFS_KERBEROS_KEYTAB is not defined" >/dev/null
else
 echo "PACK_HDFS_KERBEROS_KEYTAB=${PACK_HDFS_KERBEROS_KEYTAB}" >/dev/null
fi

if [ -z ${PACK_HDFS_KERBEROS_PRINCIPAL_NAME+x} ]; then
 echo "PACK_HDFS_KERBEROS_PRINCIPAL_NAME is not defined" >/dev/null
else
 echo "PACK_HDFS_KERBEROS_PRINCIPAL_NAME=${PACK_HDFS_KERBEROS_PRINCIPAL_NAME}" >/dev/null
fi

if [ -z ${PACK_HDFS_PATH+x} ]; then
 echo "PACK_HDFS_PATH is not defined" >/dev/null
else
 echo "PACK_HDFS_PATH=${PACK_HDFS_PATH}" >/dev/null
fi

if [ -z ${PACK_HDFS_USER+x} ]; then
 echo "PACK_HDFS_USER is not defined" >/dev/null
else
 echo "PACK_HDFS_USER=${PACK_HDFS_USER}" >/dev/null
fi

if [ -z ${PACK_LOCAL+x} ]; then
 echo "PACK_LOCAL is not defined" >/dev/null
else
 echo "PACK_LOCAL=${PACK_LOCAL}" >/dev/null
fi

if [ -z ${PACK_LOG4J_CONFIG+x} ]; then
 echo "PACK_LOG4J_CONFIG is not defined" >/dev/null
else
 echo "PACK_LOG4J_CONFIG=${PACK_LOG4J_CONFIG}" >/dev/null
fi

if [ -z ${PACK_AGENT_JAVA_HEAP+x} ]; then
 export PACK_AGENT_JAVA_HEAP="256m" >/dev/null
else
 echo "PACK_AGENT_JAVA_HEAP=${PACK_AGENT_JAVA_HEAP}" >/dev/null
fi

if [ -z ${PACK_COMPACTOR_JAVA_HEAP+x} ]; then
 export PACK_COMPACTOR_JAVA_HEAP="1G" >/dev/null
else
 echo "PACK_COMPACTOR_JAVA_HEAP=${PACK_COMPACTOR_JAVA_HEAP}" >/dev/null
fi

if [ -z ${PACK_CLI_JAVA_HEAP+x} ]; then
 export PACK_CLI_JAVA_HEAP="128m" >/dev/null
else
 echo "PACK_CLI_JAVA_HEAP=${PACK_CLI_JAVA_HEAP}" >/dev/null
fi

CMD=$1

if [ -z ${JAVA_HOME+x} ] ; then
  JAVA_CMD="java"
else
  JAVA_CMD="${JAVA_HOME}/bin/java"
fi

case $CMD in
  (agent)
    JAVA_OPTIONS="${JAVA_OPTIONS} -Xmx${PACK_AGENT_JAVA_HEAP} -Xms${PACK_AGENT_JAVA_HEAP}"
    exec -a pack-agent ${JAVA_CMD} ${JAVA_OPTIONS} pack.block.server.BlockPackServer
    ;;
  (compactor)
    JAVA_OPTIONS="${JAVA_OPTIONS} -Xmx${PACK_COMPACTOR_JAVA_HEAP} -Xms${PACK_COMPACTOR_JAVA_HEAP}"
    exec -a pack-compactor ${JAVA_CMD} ${JAVA_OPTIONS} pack.block.blockstore.compactor.PackCompactorServer
    ;;
  (cli)
    JAVA_OPTIONS="${JAVA_OPTIONS} -Xmx${PACK_CLI_JAVA_HEAP} -Xms${PACK_CLI_JAVA_HEAP}"
    exec -a pack-cli ${JAVA_CMD} ${JAVA_OPTIONS} pack.block.blockstore.cli.PackCli "${@:2}"
    ;;
  (*)
    echo "No command ${CMD}"
    ;;
esac
