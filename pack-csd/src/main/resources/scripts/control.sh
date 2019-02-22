#!/bin/bash
set -e
set -x

mkdir -p /var/log/pack

export HDFS_CONF_DIR="./hadoop-conf/"
export PACK_LOG4J_CONFIG="./log4j.properties"

if [ -z ${PACK_HDFS_KERBEROS_KEYTAB+x} ]; then
 export PACK_HDFS_USER=hdfs
 export HADOOP_USER_NAME=hdfs
else
 export PACK_HDFS_KERBEROS_KEYTAB="./pack.keytab"
 export PACK_HDFS_KERBEROS_PRINCIPAL_NAME="${SOME_ENV_VAR}"
fi

CMD=$1

case $CMD in
  (server)
    exec -a pack ${PACK_PARCEL_PATH}/bin/run.sh server
    ;;
  (compactor)
    exec -a pack ${PACK_PARCEL_PATH}/bin/run.sh compactor
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac
