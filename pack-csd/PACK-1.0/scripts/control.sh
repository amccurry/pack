#!/bin/bash
set -e
set -x

mkdir -p /var/log/pack

export HDFS_CONF_DIR="./hadoop-conf/"
export PACK_LOG4J_CONFIG="./log4j.properties"
export PACK_ZOOKEEPER_CONNECTION_STR="${ZK_QUORUM}${PACK_ZK_CHROOT}"

if [ -z ${PACK_HDFS_KERBEROS_KEYTAB+x} ]; then
 export PACK_HDFS_USER=hdfs
 export HADOOP_USER_NAME=hdfs
#else
#export PACK_HDFS_KERBEROS_KEYTAB
#export PACK_HDFS_KERBEROS_PRINCIPAL_NAME
fi

CMD=$1

case $CMD in
  (pack)
    exec -a pack ${PACK_PARCEL_PATH}/bin/run.sh pack
    ;;
  (compaction)
    exec -a pack ${PACK_PARCEL_PATH}/bin/run.sh compaction
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac
