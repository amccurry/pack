#!/bin/bash
set -e
set -x

mkdir -p ${WAL_CACHE_DIR}

export HDFS_CONF_PATH="./hadoop-conf/"
export PACK_LOG4J_CONFIG="./log4j.properties"
export KAFKA_ZK_CONNECTION="${ZK_QUORUM}${KAFKA_ZK_CHROOT}"
export PACK_ISCSI_ADDRESS="$(hostname)"
export ZK_CONNECTION="${ZK_QUORUM}${PACK_ZK_CHROOT}"
export WRITE_BLOCK_MONITOR_BIND_ADDRESS="$(hostname)"
export WRITE_BLOCK_MONITOR_ADDRESS="$(hostname)"

CMD=$1

case $CMD in
  (iscsi)
    exec -a pack ${PACK_PARCEL}/bin/run.sh iscsi
    ;;
  (compactor)
    exec -a pack ${PACK_PARCEL}/bin/run.sh compactor
    ;;
  (volume-manager)
    exec -a pack ${PACK_PARCEL}/bin/run.sh volume-manager
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac
